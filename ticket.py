from __future__ import annotations

import asyncio
import os
import shlex
import time
import re
from datetime import datetime
from typing import Dict, List, Set, Optional

import httpx
from nonebot import get_bots, get_driver, on_command, require
from nonebot.adapters.onebot.v11 import Bot, Message
from nonebot.adapters.onebot.v11.event import GroupMessageEvent
from nonebot.adapters.onebot.v11.exception import ActionFailed
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from nonebot.plugin import PluginMetadata

# Redis（可选）
try:
    import redis.asyncio as aioredis  # 推荐 >=5
except Exception:  # pragma: no cover
    aioredis = None

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler  # noqa: E402

__plugin_meta__ = PluginMetadata(
    name="票务监控 (CPP + B站 + 小芒 + 奇古米)",
    description="轮询多平台票务接口，推送变化/有票快照；支持群内启停与 Redis 持久化。",
    usage=(
        "/票务快照\n"
        "/票务监控启用\n"
        "/票务监控启用 bilibili 115413\n"
        "/票务监控启用 cpp 5476\n"
        '/票务监控启用 xm 262340 "12月27日 普通票"\n'
        "/票务监控启用 qgm 10877\n"
        "/票务监控关闭\n"
        "/票务监控关闭 bilibili 115413"
    ),
    type="application",
    homepage="https://example.com",
)

driver = get_driver()
REBUILD_THRESHOLD = 500

# ===================== 节流 / 轮询周期 =====================
SEND_INTERVAL_CHANGE_SEC = 3.0          # 有“状态变动”时的最小发送间隔
CONT_AVAIL_PUSH_MIN_INTERVAL_SEC = 9.0  # 持续有票（无变动）按时间节流的最小发送间隔

POLL_INTERVAL_CPP_SEC = 3.0
POLL_INTERVAL_BILI_SEC = 0.6
POLL_INTERVAL_XM_SEC = 0.6
POLL_INTERVAL_QGM_SEC = 0.6

BILI_VERSION = 134
BILI_REQUEST_SOURCE = "pc-new"

_cpp_req_count = 0
_bili_req_count = 0
_xm_req_count = 0
_qgm_req_count = 0

_cpp_rebuild_lock = asyncio.Lock()
_bili_rebuild_lock = asyncio.Lock()
_xm_rebuild_lock = asyncio.Lock()
_qgm_rebuild_lock = asyncio.Lock()

# —— CPP Cookie ——（按需手动填写）
CPP_JSESSIONID = "000"
CPP_TOKEN = "000"

# —— B站 Cookie ——（按需手动填写）
BILI_SESSDATA = "000"
BILI_BILI_TICKET = "000"
BILI_DEDEUSERID = "000"
BILI_DEDEUSERID_CKMD5 = "000"
BILI_SID = "000"

# —— Redis（可选）——
# 优先级：代码内显式填写 > NoneBot 配置 > 环境变量 > 本机默认 Redis。
REDIS_URL: Optional[str] = None      # 例如 "redis://:password@127.0.0.1:6379/0"
REDIS_PREFIX: str = "ticket_watch"
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/0"

# ===================== 动态配置（内存 + Redis 持久化） =====================
ENABLED_GROUPS: Set[int] = set()
CONFIG_CPP_EVENT_IDS: Set[str] = set()
CONFIG_BILI_PROJECT_IDS: Set[int] = set()
CONFIG_XM_FIXED_META: Dict[int, tuple[str, str]] = {}   # goods_id -> (date, ticket_type)
CONFIG_QGM_GOODS_IDS: Set[int] = set()
CONFIG_PUSH_GROUPS: Dict[str, Dict[str, Set[int]]] = {
    "cpp": {},
    "bilibili": {},
    "xm": {},
    "qgm": {},
}

_redis: "aioredis.Redis | None" = None

# ===================== 工具函数 =====================
def _strip_quotes(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    if (len(s) >= 2) and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1]
    return s

def _mask(s: Optional[str], keep: int = 6) -> str:
    if not s:
        return "(empty)"
    return s[:keep] + "…" + s[-keep:]

def _config_or_env_value(*names: str) -> Optional[str]:
    for name in names:
        value = getattr(driver.config, name, None)
        if value:
            return str(value).strip()
        value = os.getenv(name.upper())
        if value:
            return value.strip()
    return None

def _resolve_redis_url() -> Optional[str]:
    configured = REDIS_URL or _config_or_env_value("ticket_watch_redis_url", "redis_url")
    return configured or DEFAULT_REDIS_URL

def _short_date_cn(s: str) -> str:
    m = re.match(r'^\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*$', s or '')
    if m:
        return f"{int(m.group(2))}月{int(m.group(3))}日"
    return s or ''

def _split_xm_meta(meta: str) -> tuple[str, str]:
    meta = (meta or "").strip()
    parts = meta.split(maxsplit=1)
    if len(parts) != 2:
        raise ValueError("小芒展示信息格式必须为：\"日期 票种\"，例如 \"12月27日 普通票\"")
    return parts[0].strip(), parts[1].strip()

def _build_bili_params(project_id: int) -> dict:
    return {
        "version": BILI_VERSION,
        "id": int(project_id),
        "project_id": int(project_id),
        "requestSource": BILI_REQUEST_SOURCE,
    }

def _normalize_platform_name(platform: str) -> str:
    p = (platform or "").strip().lower()
    mapping = {
        "bilibili": "bilibili",
        "bili": "bilibili",
        "bl": "bilibili",
        "cpp": "cpp",
        "xm": "xm",
        "qgm": "qgm",
        "qg": "qgm",
        "qigumi": "qgm",
    }
    if p not in mapping:
        raise ValueError(f"未知平台：{platform}")
    return mapping[p]

def _redis_key_enabled() -> str:
    return f"{REDIS_PREFIX}:enabled_groups"

def _redis_key_cpp() -> str:
    return f"{REDIS_PREFIX}:config:cpp"

def _redis_key_bili() -> str:
    return f"{REDIS_PREFIX}:config:bilibili"

def _redis_key_xm() -> str:
    return f"{REDIS_PREFIX}:config:xm"

def _redis_key_qgm() -> str:
    return f"{REDIS_PREFIX}:config:qgm"

def _redis_key_config_groups(platform: str, target_id: int | str) -> str:
    return f"{REDIS_PREFIX}:config_groups:{platform}:{target_id}"

def _target_key(platform: str, target_id: int | str) -> str:
    platform = _normalize_platform_name(platform)
    if platform == "cpp":
        return str(target_id).strip()
    return str(int(target_id))

def _ensure_push_group_bucket(platform: str, target_id: int | str) -> Set[int]:
    platform = _normalize_platform_name(platform)
    key = _target_key(platform, target_id)
    return CONFIG_PUSH_GROUPS.setdefault(platform, {}).setdefault(key, set())

def _get_push_groups(platform: str, target_id: int | str) -> Set[int]:
    platform = _normalize_platform_name(platform)
    key = _target_key(platform, target_id)
    groups = CONFIG_PUSH_GROUPS.get(platform, {}).get(key)
    if groups is None:
        return set()
    return set(groups) & set(ENABLED_GROUPS)

def _format_monitor_summary_for_group(group_id: int, include_empty: bool = False) -> str:
    lines: list[str] = []

    cpp_ids = sorted(x for x in CONFIG_CPP_EVENT_IDS if group_id in _get_push_groups("cpp", x))
    if cpp_ids or include_empty:
        lines.append(f"CPP: {'、'.join(cpp_ids) or '（无）'}")

    bili_ids = sorted(x for x in CONFIG_BILI_PROJECT_IDS if group_id in _get_push_groups("bilibili", x))
    if bili_ids or include_empty:
        lines.append(f"B站: {'、'.join(str(x) for x in bili_ids) or '（无）'}")

    xm_ids = sorted(x for x in CONFIG_XM_FIXED_META if group_id in _get_push_groups("xm", x))
    if xm_ids or include_empty:
        xm_text = "、".join(f'{gid}({CONFIG_XM_FIXED_META[gid][0]} {CONFIG_XM_FIXED_META[gid][1]})' for gid in xm_ids)
        lines.append(f"小芒: {xm_text or '（无）'}")

    qgm_ids = sorted(x for x in CONFIG_QGM_GOODS_IDS if group_id in _get_push_groups("qgm", x))
    if qgm_ids or include_empty:
        lines.append(f"奇古米: {'、'.join(str(x) for x in qgm_ids) or '（无）'}")

    return "\n".join(lines) if lines else "（无）"

def _job_id(platform: str) -> str:
    return {
        "cpp": "ticket_watch_cpp",
        "bilibili": "ticket_watch_bili",
        "xm": "ticket_watch_xm",
        "qgm": "ticket_watch_qgm",
    }[platform]

def _platform_has_config(platform: str) -> bool:
    if platform == "cpp":
        return bool(CONFIG_CPP_EVENT_IDS)
    if platform == "bilibili":
        return bool(CONFIG_BILI_PROJECT_IDS)
    if platform == "xm":
        return bool(CONFIG_XM_FIXED_META)
    if platform == "qgm":
        return bool(CONFIG_QGM_GOODS_IDS)
    return False

def _has_any_monitor_config() -> bool:
    return any([
        CONFIG_CPP_EVENT_IDS,
        CONFIG_BILI_PROJECT_IDS,
        CONFIG_XM_FIXED_META,
        CONFIG_QGM_GOODS_IDS,
    ])

def _format_monitor_summary(include_empty: bool = False) -> str:
    lines: list[str] = []
    if CONFIG_CPP_EVENT_IDS or include_empty:
        cpp_text = "、".join(sorted(CONFIG_CPP_EVENT_IDS)) or "（无）"
        lines.append(f"CPP: {cpp_text}")
    if CONFIG_BILI_PROJECT_IDS or include_empty:
        bili_text = "、".join(str(x) for x in sorted(CONFIG_BILI_PROJECT_IDS)) or "（无）"
        lines.append(f"B站: {bili_text}")
    if CONFIG_XM_FIXED_META or include_empty:
        xm_text = "、".join(
            f'{gid}({meta[0]} {meta[1]})'
            for gid, meta in sorted(CONFIG_XM_FIXED_META.items(), key=lambda x: x[0])
        ) or "（无）"
        lines.append(f"小芒: {xm_text}")
    if CONFIG_QGM_GOODS_IDS or include_empty:
        qgm_text = "、".join(str(x) for x in sorted(CONFIG_QGM_GOODS_IDS)) or "（无）"
        lines.append(f"奇古米: {qgm_text}")
    return "\n".join(lines) if lines else "（无）"

def _reset_cpp_state(event_id: Optional[str] = None):
    global _cpp_prev_status, _cpp_last_send, _cpp_last_avail_push
    if event_id is None:
        _cpp_prev_status.clear()
        _cpp_last_send = 0.0
        _cpp_last_avail_push = 0.0
        return
    _cpp_prev_status.pop(str(event_id), None)

def _reset_bili_state(project_id: Optional[int] = None):
    global _bili_prev_status, _bili_last_send, _bili_last_avail_push, _bili_next_index
    if project_id is None:
        _bili_prev_status.clear()
        _bili_last_send = 0.0
        _bili_last_avail_push = 0.0
        _bili_next_index = 0
        return
    _bili_prev_status.pop(int(project_id), None)
    _bili_next_index = 0

def _reset_xm_state(goods_id: Optional[int] = None):
    global _xm_prev_status, _xm_last_send, _xm_last_avail_push, _xm_next_index
    if goods_id is None:
        _xm_prev_status.clear()
        _xm_last_send = 0.0
        _xm_last_avail_push = 0.0
        _xm_next_index = 0
        return
    _xm_prev_status.pop(int(goods_id), None)
    _xm_next_index = 0

def _reset_qgm_state(goods_id: Optional[int] = None):
    global _qgm_prev_status, _qgm_last_send, _qgm_last_avail_push, _qgm_next_index
    if goods_id is None:
        _qgm_prev_status.clear()
        _qgm_last_send = 0.0
        _qgm_last_avail_push = 0.0
        _qgm_next_index = 0
        return
    _qgm_prev_status.pop(int(goods_id), None)
    _qgm_next_index = 0

# ===================== Redis：启用群 =====================
async def _redis_load_enabled_groups():
    global ENABLED_GROUPS
    if not _redis:
        return
    try:
        members = await _redis.smembers(_redis_key_enabled())
        gids: Set[int] = set()
        for m in members:
            try:
                if isinstance(m, bytes):
                    m = m.decode("utf-8", "ignore")
                gids.add(int(m))
            except Exception:
                continue
        ENABLED_GROUPS |= gids
        if gids:
            logger.info(f"已从 Redis 加载启用群：{sorted(list(gids))}")
    except Exception as e:
        logger.warning(f"从 Redis 读取启用群失败：{e}")

async def _redis_add_enabled_group(gid: int):
    if not _redis:
        return
    try:
        await _redis.sadd(_redis_key_enabled(), gid)
    except Exception as e:
        logger.warning(f"写入 Redis 失败（添加群）：{e}")

async def _redis_remove_enabled_group(gid: int):
    if not _redis:
        return
    try:
        await _redis.srem(_redis_key_enabled(), gid)
    except Exception as e:
        logger.warning(f"写入 Redis 失败（移除群）：{e}")

async def _redis_add_config_group(platform: str, target_id: int | str, gid: int):
    if not _redis:
        return
    try:
        await _redis.sadd(_redis_key_config_groups(platform, target_id), gid)
    except Exception as e:
        logger.warning(f"写入 Redis 失败（添加配置推送群 platform={platform} id={target_id} gid={gid}）：{e}")

async def _redis_remove_config_group(platform: str, target_id: int | str, gid: int):
    if not _redis:
        return
    try:
        await _redis.srem(_redis_key_config_groups(platform, target_id), gid)
    except Exception as e:
        logger.warning(f"写入 Redis 失败（移除配置推送群 platform={platform} id={target_id} gid={gid}）：{e}")

async def _redis_delete_config_groups(platform: str, target_id: int | str):
    if not _redis:
        return
    try:
        await _redis.delete(_redis_key_config_groups(platform, target_id))
    except Exception as e:
        logger.warning(f"写入 Redis 失败（删除配置推送群 platform={platform} id={target_id}）：{e}")

# ===================== Redis：监控配置 =====================
async def _redis_load_monitor_configs():
    global CONFIG_CPP_EVENT_IDS, CONFIG_BILI_PROJECT_IDS, CONFIG_XM_FIXED_META, CONFIG_QGM_GOODS_IDS, CONFIG_PUSH_GROUPS
    if not _redis:
        return

    try:
        cpp_members = await _redis.smembers(_redis_key_cpp())
        bili_members = await _redis.smembers(_redis_key_bili())
        qgm_members = await _redis.smembers(_redis_key_qgm())
        xm_map = await _redis.hgetall(_redis_key_xm())

        CONFIG_CPP_EVENT_IDS = {
            str(x).strip()
            for x in cpp_members or []
            if str(x).strip()
        }

        CONFIG_BILI_PROJECT_IDS = set()
        for x in bili_members or []:
            try:
                CONFIG_BILI_PROJECT_IDS.add(int(x))
            except Exception:
                continue

        CONFIG_QGM_GOODS_IDS = set()
        for x in qgm_members or []:
            try:
                CONFIG_QGM_GOODS_IDS.add(int(x))
            except Exception:
                continue

        CONFIG_XM_FIXED_META = {}
        for k, v in (xm_map or {}).items():
            try:
                gid = int(k)
                date, ticket_type = _split_xm_meta(str(v))
                CONFIG_XM_FIXED_META[gid] = (date, ticket_type)
            except Exception:
                continue

        CONFIG_PUSH_GROUPS = {"cpp": {}, "bilibili": {}, "xm": {}, "qgm": {}}
        for platform, target_ids in (
            ("cpp", CONFIG_CPP_EVENT_IDS),
            ("bilibili", CONFIG_BILI_PROJECT_IDS),
            ("xm", CONFIG_XM_FIXED_META.keys()),
            ("qgm", CONFIG_QGM_GOODS_IDS),
        ):
            for target_id in target_ids:
                members = await _redis.smembers(_redis_key_config_groups(platform, target_id))
                groups: Set[int] = set()
                for m in members or []:
                    try:
                        groups.add(int(m))
                    except Exception:
                        continue
                if not groups:
                    groups = set(ENABLED_GROUPS)
                CONFIG_PUSH_GROUPS[platform][_target_key(platform, target_id)] = groups

        if _has_any_monitor_config():
            logger.info("已从 Redis 加载监控配置：\n%s", _format_monitor_summary())
        else:
            logger.info("Redis 中未发现任何监控配置，启动时不注册轮询任务。")
    except Exception as e:
        logger.warning(f"从 Redis 读取监控配置失败：{e}")

async def _redis_add_monitor_config(platform: str, target_id: int | str, extra: Optional[str] = None):
    if not _redis:
        return
    try:
        if platform == "cpp":
            await _redis.sadd(_redis_key_cpp(), str(target_id))
        elif platform == "bilibili":
            await _redis.sadd(_redis_key_bili(), int(target_id))
        elif platform == "qgm":
            await _redis.sadd(_redis_key_qgm(), int(target_id))
        elif platform == "xm":
            if not extra:
                raise ValueError("小芒配置必须提供展示信息")
            await _redis.hset(_redis_key_xm(), int(target_id), extra.strip())
    except Exception as e:
        logger.warning(f"写入 Redis 失败（添加监控配置 platform={platform} id={target_id}）：{e}")

async def _redis_remove_monitor_config(platform: str, target_id: int | str):
    if not _redis:
        return
    try:
        if platform == "cpp":
            await _redis.srem(_redis_key_cpp(), str(target_id))
        elif platform == "bilibili":
            await _redis.srem(_redis_key_bili(), int(target_id))
        elif platform == "qgm":
            await _redis.srem(_redis_key_qgm(), int(target_id))
        elif platform == "xm":
            await _redis.hdel(_redis_key_xm(), int(target_id))
    except Exception as e:
        logger.warning(f"写入 Redis 失败（移除监控配置 platform={platform} id={target_id}）：{e}")

async def _add_config_push_group(platform: str, target_id: int | str, gid: int):
    groups = _ensure_push_group_bucket(platform, target_id)
    if gid not in groups:
        groups.add(gid)
        await _redis_add_config_group(platform, target_id, gid)

async def _remove_config_push_group(platform: str, target_id: int | str, gid: int) -> bool:
    platform = _normalize_platform_name(platform)
    key = _target_key(platform, target_id)
    groups = CONFIG_PUSH_GROUPS.get(platform, {}).get(key)
    if not groups or gid not in groups:
        return False
    groups.remove(gid)
    await _redis_remove_config_group(platform, target_id, gid)
    return True

def _config_has_push_groups(platform: str, target_id: int | str) -> bool:
    platform = _normalize_platform_name(platform)
    key = _target_key(platform, target_id)
    return bool(CONFIG_PUSH_GROUPS.get(platform, {}).get(key))

async def _add_group_to_all_existing_configs(gid: int):
    for target_id in sorted(CONFIG_CPP_EVENT_IDS):
        await _add_config_push_group("cpp", target_id, gid)
    for target_id in sorted(CONFIG_BILI_PROJECT_IDS):
        await _add_config_push_group("bilibili", target_id, gid)
    for target_id in sorted(CONFIG_XM_FIXED_META.keys()):
        await _add_config_push_group("xm", target_id, gid)
    for target_id in sorted(CONFIG_QGM_GOODS_IDS):
        await _add_config_push_group("qgm", target_id, gid)

async def _remove_group_from_all_config_buckets(gid: int):
    for platform, target_map in CONFIG_PUSH_GROUPS.items():
        for target_id in list(target_map.keys()):
            if gid in target_map[target_id]:
                target_map[target_id].remove(gid)
                await _redis_remove_config_group(platform, target_id, gid)

async def _add_monitor_config(platform: str, target_id: int | str, extra: Optional[str] = None):
    platform = _normalize_platform_name(platform)
    changed = False

    if platform == "cpp":
        target = str(target_id).strip()
        if target and target not in CONFIG_CPP_EVENT_IDS:
            CONFIG_CPP_EVENT_IDS.add(target)
            _reset_cpp_state(target)
            changed = True

    elif platform == "bilibili":
        target = int(target_id)
        if target not in CONFIG_BILI_PROJECT_IDS:
            CONFIG_BILI_PROJECT_IDS.add(target)
            _reset_bili_state(target)
            changed = True

    elif platform == "qgm":
        target = int(target_id)
        if target not in CONFIG_QGM_GOODS_IDS:
            CONFIG_QGM_GOODS_IDS.add(target)
            _reset_qgm_state(target)
            changed = True

    elif platform == "xm":
        target = int(target_id)
        if not extra:
            raise ValueError("小芒配置必须提供展示信息，例如：/票务监控启用 xm 262340 \"12月27日 普通票\"")
        date, ticket_type = _split_xm_meta(extra)
        old = CONFIG_XM_FIXED_META.get(target)
        new = (date, ticket_type)
        if old != new:
            CONFIG_XM_FIXED_META[target] = new
            _reset_xm_state(target)
            changed = True

    if changed:
        await _redis_add_monitor_config(platform, target_id, extra=extra)
        await _sync_platform_job(platform)
    return changed

async def _remove_monitor_config(platform: str, target_id: int | str):
    platform = _normalize_platform_name(platform)
    changed = False

    if platform == "cpp":
        target = str(target_id).strip()
        if target in CONFIG_CPP_EVENT_IDS:
            CONFIG_CPP_EVENT_IDS.remove(target)
            _reset_cpp_state(target)
            changed = True

    elif platform == "bilibili":
        target = int(target_id)
        if target in CONFIG_BILI_PROJECT_IDS:
            CONFIG_BILI_PROJECT_IDS.remove(target)
            _reset_bili_state(target)
            changed = True

    elif platform == "qgm":
        target = int(target_id)
        if target in CONFIG_QGM_GOODS_IDS:
            CONFIG_QGM_GOODS_IDS.remove(target)
            _reset_qgm_state(target)
            changed = True

    elif platform == "xm":
        target = int(target_id)
        if target in CONFIG_XM_FIXED_META:
            CONFIG_XM_FIXED_META.pop(target, None)
            _reset_xm_state(target)
            changed = True

    if changed:
        await _redis_remove_monitor_config(platform, target_id)
        CONFIG_PUSH_GROUPS.get(platform, {}).pop(_target_key(platform, target_id), None)
        await _redis_delete_config_groups(platform, target_id)
        await _sync_platform_job(platform)
    return changed

# ===================== HTTP 客户端 =====================
CPP_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
    "origin": "https://cp.allcpp.cn",
    "referer": "https://cp.allcpp.cn/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/135 Safari/537.36",
}
BILI_HEADERS_BASE = {
    "Accept": "*/*",
    "Referer": "https://show.bilibili.com/platform/detail.html",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}
XM_API = "https://mgecom.api.mgtv.com/goods/dsl/dynamic"
XM_HEADERS = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/135 Safari/537.36",
}
QGM_API = "https://app.qigumi.com/api/v3/goods/chooseTicketGoodsVenue"
QGM_HEADERS = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/135 Safari/537.36",
}

cpp_client: httpx.AsyncClient | None = None
bili_client: httpx.AsyncClient | None = None
xm_client: httpx.AsyncClient | None = None
qgm_client: httpx.AsyncClient | None = None

_cpp_job_lock = asyncio.Lock()
_bili_job_lock = asyncio.Lock()
_xm_job_lock = asyncio.Lock()
_qgm_job_lock = asyncio.Lock()

async def _rebuild_cpp_client():
    global cpp_client
    async with _cpp_rebuild_lock:
        try:
            if cpp_client:
                await cpp_client.aclose()
        except Exception:
            pass
        cpp_cookies = {}
        if CPP_JSESSIONID:
            cpp_cookies["JSESSIONID"] = _strip_quotes(CPP_JSESSIONID)
        if CPP_TOKEN:
            cpp_cookies["token"] = _strip_quotes(CPP_TOKEN)
        cpp_client = httpx.AsyncClient(
            headers=CPP_HEADERS,
            cookies=cpp_cookies,
            http2=False,
            timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
        )

async def _rebuild_bili_client():
    global bili_client
    async with _bili_rebuild_lock:
        try:
            if bili_client:
                await bili_client.aclose()
        except Exception:
            pass
        bili_cookies = {}
        if BILI_SESSDATA:
            bili_cookies["SESSDATA"] = BILI_SESSDATA
        if BILI_BILI_TICKET:
            bili_cookies["bili_ticket"] = BILI_BILI_TICKET
        if BILI_DEDEUSERID:
            bili_cookies["DedeUserID"] = BILI_DEDEUSERID
        if BILI_DEDEUSERID_CKMD5:
            bili_cookies["DedeUserID__ckMd5"] = BILI_DEDEUSERID_CKMD5
        if BILI_SID:
            bili_cookies["sid"] = BILI_SID
        bili_client = httpx.AsyncClient(
            headers=BILI_HEADERS_BASE,
            cookies=bili_cookies,
            http2=True,
            timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
        )

async def _rebuild_xm_client():
    global xm_client
    async with _xm_rebuild_lock:
        try:
            if xm_client:
                await xm_client.aclose()
        except Exception:
            pass
        xm_client = httpx.AsyncClient(
            headers=XM_HEADERS,
            http2=True,
            timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
        )

async def _rebuild_qgm_client():
    global qgm_client
    async with _qgm_rebuild_lock:
        try:
            if qgm_client:
                await qgm_client.aclose()
        except Exception:
            pass
        qgm_client = httpx.AsyncClient(
            headers=QGM_HEADERS,
            http2=True,
            timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
        )

@driver.on_startup
async def _init_clients_and_redis():
    """初始化各平台 HTTP 客户端与 Redis；恢复启用群与监控配置。"""
    global cpp_client, bili_client, xm_client, qgm_client, _redis

    cpp_cookies = {}
    if CPP_JSESSIONID:
        cpp_cookies["JSESSIONID"] = _strip_quotes(CPP_JSESSIONID)
    if CPP_TOKEN:
        cpp_cookies["token"] = _strip_quotes(CPP_TOKEN)

    cpp_client = httpx.AsyncClient(
        headers=CPP_HEADERS,
        cookies=cpp_cookies,
        http2=False,
        timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
    )
    logger.info(
        "CPP cookies 已载入: JSESSIONID=%s token=%s",
        _mask(cpp_cookies.get("JSESSIONID")),
        _mask(cpp_cookies.get("token")),
    )

    bili_cookies = {}
    if BILI_SESSDATA:
        bili_cookies["SESSDATA"] = BILI_SESSDATA
    if BILI_BILI_TICKET:
        bili_cookies["bili_ticket"] = BILI_BILI_TICKET
    if BILI_DEDEUSERID:
        bili_cookies["DedeUserID"] = BILI_DEDEUSERID
    if BILI_DEDEUSERID_CKMD5:
        bili_cookies["DedeUserID__ckMd5"] = BILI_DEDEUSERID_CKMD5
    if BILI_SID:
        bili_cookies["sid"] = BILI_SID

    bili_client = httpx.AsyncClient(
        headers=BILI_HEADERS_BASE,
        cookies=bili_cookies,
        http2=True,
        timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
    )

    xm_client = httpx.AsyncClient(
        headers=XM_HEADERS,
        http2=True,
        timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
    )

    qgm_client = httpx.AsyncClient(
        headers=QGM_HEADERS,
        http2=True,
        timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
    )

    redis_url = _resolve_redis_url()
    if redis_url:
        if not aioredis:
            logger.warning("检测到 Redis 配置，但未安装 redis>=5；将仅使用内存配置。")
        else:
            try:
                _redis = aioredis.from_url(redis_url, decode_responses=True)
                await _redis.ping()
                logger.info("Redis 已连接，用于启用群与监控配置持久化：%s", redis_url)
                await _redis_load_enabled_groups()
                await _redis_load_monitor_configs()
            except Exception as e:
                _redis = None
                logger.warning(f"Redis 连接失败，将仅使用内存配置：{e}")

    await _sync_all_jobs()
    logger.opt(colors=True).info("<g>nonebot_plugin_ticket_watch</g> 初始化完成")

@driver.on_shutdown
async def _close_clients_and_redis():
    global cpp_client, bili_client, xm_client, qgm_client, _redis
    try:
        if cpp_client:
            await cpp_client.aclose()
        if bili_client:
            await bili_client.aclose()
        if xm_client:
            await xm_client.aclose()
        if qgm_client:
            await qgm_client.aclose()
    finally:
        cpp_client = None
        bili_client = None
        xm_client = None
        qgm_client = None

    try:
        if _redis:
            await _redis.close()
    except Exception:
        pass
    finally:
        _redis = None

# ===================== 调度同步 =====================
async def _sync_platform_job(platform: str):
    platform = _normalize_platform_name(platform)
    job_id = _job_id(platform)
    job = scheduler.get_job(job_id)

    if platform == "cpp":
        func = cpp_tick_and_maybe_send
        seconds = POLL_INTERVAL_CPP_SEC
        label = "CPP"
    elif platform == "bilibili":
        func = bili_tick_and_maybe_send
        seconds = POLL_INTERVAL_BILI_SEC
        label = "B站"
    elif platform == "xm":
        func = xm_tick_and_maybe_send
        seconds = POLL_INTERVAL_XM_SEC
        label = "小芒"
    else:
        func = qgm_tick_and_maybe_send
        seconds = POLL_INTERVAL_QGM_SEC
        label = "奇古米"

    if _platform_has_config(platform):
        if job is None:
            scheduler.add_job(
                func,
                "interval",
                seconds=seconds,
                id=job_id,
                max_instances=2,
                coalesce=True,
            )
            logger.info("%s 票务监控任务已启动，轮询周期 %.1fs", label, seconds)
    else:
        if job is not None:
            scheduler.remove_job(job_id)
            logger.info("%s 票务监控任务已停止（当前无配置）", label)

async def _sync_all_jobs():
    await _sync_platform_job("cpp")
    await _sync_platform_job("bilibili")
    await _sync_platform_job("xm")
    await _sync_platform_job("qgm")

# ===================== CPP 轮询逻辑 =====================
CPP_URL = "https://www.allcpp.cn/allcpp/ticket/getTicketTypeList.do"

_cpp_prev_status: Dict[str, Dict[str, str]] = {}  # event_id -> {key: status}
_cpp_last_send = 0.0
_cpp_last_avail_push = 0.0

async def cpp_fetch_raw(event_id: str) -> dict:
    global _cpp_req_count
    _cpp_req_count += 1
    assert cpp_client is not None
    params = {"eventMainId": str(event_id)}
    try:
        r = await cpp_client.get(CPP_URL, params=params, follow_redirects=False)
        if r.status_code != 200:
            logger.warning(f"CPP 响应异常 event_id={event_id} status={r.status_code} text={r.text[:200]}")
            return {}
        return r.json()
    except Exception as e:
        logger.warning(f"CPP 请求失败 event_id={event_id}: {e}")
        return {}

async def cpp_fetch_once_for(event_id: str) -> tuple[list[str], list[str], Dict[str, str]]:
    data = await cpp_fetch_raw(event_id)
    change_lines: list[str] = []
    available_lines: list[str] = []
    curr_map: Dict[str, str] = {}
    prev_map = _cpp_prev_status.get(str(event_id), {})

    for t in data.get("ticketTypeList", []) or []:
        key = str(t.get("id") or t.get("ticketTypeId") or f"{t.get('ticketName', '')}|{t.get('square', '')}")
        name = (t.get("ticketName") or "").strip()
        try:
            rem = int(t.get("remainderNum", 0) or 0)
        except Exception:
            rem = 0
        if rem < 0:
            rem = 0
        try:
            open_timer = int(t.get("openTimer", 0) or 0)
        except Exception:
            open_timer = 0

        if open_timer > 0:
            status_label = f"未开售({rem})"
            on_sale = False
        elif rem > 0:
            status_label = f"可购买({rem})"
            on_sale = True
        else:
            status_label = "已售罄(0)"
            on_sale = False

        curr_map[key] = status_label
        line = f"{name} {status_label}"

        if prev_map.get(key) != status_label:
            change_lines.append(line)
        if on_sale:
            available_lines.append(line)

    return change_lines, available_lines, curr_map

async def cpp_tick_and_maybe_send():
    global _cpp_last_send, _cpp_last_avail_push, _cpp_req_count
    if _cpp_job_lock.locked():
        return
    async with _cpp_job_lock:
        if not CONFIG_CPP_EVENT_IDS:
            return

        updates: Dict[str, tuple[list[str], list[str]]] = {}
        curr_all: Dict[str, Dict[str, str]] = {}

        for event_id in sorted(CONFIG_CPP_EVENT_IDS):
            chg, avail, curr = await cpp_fetch_once_for(event_id)
            curr_all[event_id] = curr
            updates[event_id] = (chg, avail)
        chg_all = [line for chg, _ in updates.values() for line in chg]
        avail_all = [line for _, avail in updates.values() for line in avail]

        now = time.time()
        has_change = bool(chg_all)
        has_avail = bool(avail_all)

        should_send = False
        if has_change and (now - _cpp_last_send) >= SEND_INTERVAL_CHANGE_SEC:
            should_send = True
        elif (not has_change) and has_avail and (now - _cpp_last_avail_push) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC \
                and (now - _cpp_last_send) >= 0.1:
            should_send = True
            _cpp_last_avail_push = now

        if should_send:
            ts = datetime.now().strftime("%Y.%m.%d %H:%M:%S")
            for event_id, (chg, avail) in updates.items():
                merged = list(dict.fromkeys(chg + avail))
                if not merged:
                    continue
                body = [
                    f"CPP项目{event_id}状态更新：",
                    *merged,
                    ts,
                ]
                await safe_broadcast("\n".join(body), _get_push_groups("cpp", event_id))
            _cpp_last_send = now
            _cpp_prev_status.update(curr_all)

    if _cpp_req_count >= REBUILD_THRESHOLD:
        await _rebuild_cpp_client()
        _cpp_req_count = 0

# ===================== B站 轮询逻辑 =====================
BILI_API = "https://show.bilibili.com/api/ticket/project/getV2"

_bili_prev_status: Dict[int, Dict[str, str]] = {}
_bili_last_send = 0.0
_bili_last_avail_push = 0.0
_bili_next_index = 0

def _is_on_sale(flag) -> bool:
    try:
        return int(flag) == 2
    except Exception:
        return False

async def bili_fetch_raw(params: dict) -> dict:
    global _bili_req_count
    _bili_req_count += 1
    assert bili_client is not None
    headers = dict(bili_client.headers)
    ref_id = params.get("id") or params.get("project_id")
    if ref_id:
        headers["Referer"] = f"https://show.bilibili.com/platform/detail.html?id={ref_id}"
    try:
        r = await bili_client.get(BILI_API, params=params, headers=headers, follow_redirects=False)
        return r.json() if r.status_code == 200 else {}
    except Exception as e:
        logger.warning(f"B站请求失败 project_id={params.get('project_id')}: {e}")
        return {}

async def bili_fetch_once_for(project_id: int) -> tuple[list[str], list[str], Dict[str, str]]:
    info = await bili_fetch_raw(_build_bili_params(project_id))
    data = info.get("data", {}) or {}
    curr_status: Dict[str, str] = {}
    change_lines: list[str] = []
    available_lines: list[str] = []
    prev_map = _bili_prev_status.get(int(project_id), {})

    for screen in data.get("screen_list", []) or []:
        for ticket in screen.get("ticket_list", []) or []:
            date = (ticket.get("screen_name") or screen.get("name") or "").strip()
            desc = (ticket.get("desc") or "").strip()

            sale_flag = (ticket.get("sale_flag") or {})
            flag = sale_flag.get("number") or ticket.get("sale_flag_number")
            display = sale_flag.get("display_name", "") or ""

            count = ticket.get("num", 0)
            try:
                count = int(count)
            except Exception:
                count = 0
            if count < 0:
                count = 0

            display_fmt = f"{display}({count})"
            key = f"{date}||{desc}"

            curr_status[key] = display
            if prev_map.get(key) != display:
                change_lines.append(f"{date} {desc} {display_fmt}")

            if _is_on_sale(flag):
                available_lines.append(f"{date} {desc} {display_fmt}")

    return change_lines, available_lines, curr_status

async def bili_tick_and_maybe_send():
    global _bili_last_send, _bili_last_avail_push, _bili_next_index, _bili_req_count

    if _bili_job_lock.locked():
        return
    async with _bili_job_lock:
        project_ids = sorted(CONFIG_BILI_PROJECT_IDS)
        if not project_ids:
            return

        if _bili_next_index >= len(project_ids):
            _bili_next_index = 0
        project_id = project_ids[_bili_next_index % len(project_ids)]
        _bili_next_index = (_bili_next_index + 1) % len(project_ids)

        chg, avail, curr = await bili_fetch_once_for(project_id)

        now = time.time()

        should_send = False
        if chg and (now - _bili_last_send) >= SEND_INTERVAL_CHANGE_SEC:
            should_send = True
        elif (not chg) and avail and (now - _bili_last_avail_push) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC \
                and (now - _bili_last_send) >= 0.1:
            should_send = True
            _bili_last_avail_push = now

        if should_send:
            merged = list(dict.fromkeys(chg + avail))
            body = [
                f"B站项目{project_id}状态更新：",
                *merged,
                datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
            ]
            await safe_broadcast("\n".join(body), _get_push_groups("bilibili", project_id))
            _bili_last_send = now
            _bili_prev_status[project_id] = curr

    if _bili_req_count >= REBUILD_THRESHOLD:
        await _rebuild_bili_client()
        _bili_req_count = 0

# ===================== 小芒 轮询逻辑 =====================
_xm_prev_status: Dict[int, Dict[str, str]] = {}  # goods_id -> { "日期||票种": "预售中|已售罄" }
_xm_last_send = 0.0
_xm_last_avail_push = 0.0
_xm_next_index = 0

async def xm_fetch_raw(goods_id: int) -> dict:
    global _xm_req_count
    _xm_req_count += 1
    assert xm_client is not None
    try:
        r = await xm_client.get(XM_API, params={"goods_id": str(goods_id)}, follow_redirects=False)
        if r.status_code != 200:
            logger.warning(f"小芒响应异常 goods_id={goods_id} status={r.status_code} text={r.text[:200]}")
            return {}
        return r.json()
    except Exception as e:
        logger.warning(f"小芒请求失败 goods_id={goods_id}: {e}")
        return {}

def _xm_build_lines(info: dict, goods_id: Optional[int] = None) -> Dict[str, str]:
    origin = (info or {}).get("originData") or {}
    sku_list = origin.get("sku_list") or []
    site_goods = {
        (g.get("title") or "").strip(): (g.get("sub_title") or "").strip()
        for g in (origin.get("ticket_info", {}) or {}).get("ticket_site_goods", []) or []
    }

    fixed_date, fixed_type = ("", "")
    if goods_id is not None and goods_id in CONFIG_XM_FIXED_META:
        fixed_date, fixed_type = CONFIG_XM_FIXED_META[goods_id]

    curr_map: Dict[str, str] = {}
    for sku in sku_list:
        date = (sku.get("spec1") or "").strip() or fixed_date
        ticket_type = site_goods.get(date, "").strip() or fixed_type
        status_text = (sku.get("store_count_text") or "").strip()
        status_norm = "预售中" if status_text == "有货" else "已售罄"

        key = f"{date}||{ticket_type}"
        curr_map[key] = status_norm

    if not curr_map and fixed_date and fixed_type:
        curr_map[f"{fixed_date}||{fixed_type}"] = "已售罄"

    return curr_map

async def xm_fetch_once_for(goods_id: int) -> tuple[list[str], list[str], Dict[str, str]]:
    info = await xm_fetch_raw(goods_id)
    curr_map = _xm_build_lines(info, goods_id=goods_id)
    prev_map = _xm_prev_status.get(goods_id, {})
    change_lines: list[str] = []
    avail_lines: list[str] = []

    for key, status in curr_map.items():
        date, ticket_type = key.split("||", 1)
        line = f"{date} {ticket_type} {status}"
        if prev_map.get(key) != status:
            change_lines.append(line)
        if status == "预售中":
            avail_lines.append(line)

    return change_lines, avail_lines, curr_map

async def xm_tick_and_maybe_send():
    global _xm_last_send, _xm_last_avail_push, _xm_next_index, _xm_req_count
    if _xm_job_lock.locked():
        return
    async with _xm_job_lock:
        goods_ids = sorted(CONFIG_XM_FIXED_META.keys())
        if not goods_ids:
            return

        if _xm_next_index >= len(goods_ids):
            _xm_next_index = 0
        goods_id = goods_ids[_xm_next_index % len(goods_ids)]
        _xm_next_index = (_xm_next_index + 1) % len(goods_ids)

        chg, avail, curr = await xm_fetch_once_for(goods_id)
        now = time.time()

        should_send = False
        if chg and (now - _xm_last_send) >= SEND_INTERVAL_CHANGE_SEC:
            should_send = True
        elif (not chg) and avail and (now - _xm_last_avail_push) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC \
                and (now - _xm_last_send) >= 0.1:
            should_send = True
            _xm_last_avail_push = now

        if should_send:
            merged = list(dict.fromkeys([f"[{goods_id}] {line}" for line in (chg + avail)]))
            body = [
                f"小芒项目{goods_id}状态更新：",
                *merged,
                datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
            ]
            await safe_broadcast("\n".join(body), _get_push_groups("xm", goods_id))
            _xm_last_send = now
            _xm_prev_status[goods_id] = curr

    if _xm_req_count >= REBUILD_THRESHOLD:
        await _rebuild_xm_client()
        _xm_req_count = 0

# ===================== 奇古米 轮询逻辑 =====================
_qgm_prev_status: Dict[int, Dict[str, str]] = {}
_qgm_last_send = 0.0
_qgm_last_avail_push = 0.0
_qgm_next_index = 0

def _qgm_status_name(code: int) -> str:
    mapping = {
        1: "未开售",
        2: "已售罄",
        3: "预售中",
        4: "已停售",
        5: "已售罄",
    }
    return mapping.get(int(code or 0), f"状态{code}")

async def qgm_fetch_raw(goods_id: int) -> dict:
    global _qgm_req_count
    _qgm_req_count += 1
    assert qgm_client is not None
    try:
        r = await qgm_client.get(QGM_API, params={"goods_id": str(goods_id)}, follow_redirects=False)
        if r.status_code != 200:
            logger.warning(f"奇古米响应异常 goods_id={goods_id} status={r.status_code} text={r.text[:200]}")
            return {}
        return r.json()
    except Exception as e:
        logger.warning(f"奇古米请求失败 goods_id={goods_id}: {e}")
        return {}

def _qgm_build_map(info: dict) -> Dict[str, str]:
    data = (info or {}).get("b", {}) or {}
    tgd = data.get("ticket_goods_data") or {}
    venue_list = tgd.get("venue_list") or []

    curr_map: Dict[str, str] = {}
    for venue in venue_list:
        date = _short_date_cn((venue.get("venue_show_time") or venue.get("venue_name") or "").strip())
        v_status = _qgm_status_name(venue.get("button_status"))

        for sku in venue.get("ticket_sku_list") or []:
            ticket_type = (sku.get("name") or "").strip()
            key = f"{date}||{ticket_type}"
            curr_map[key] = v_status
    return curr_map

async def qgm_fetch_once_for(goods_id: int) -> tuple[list[str], list[str], Dict[str, str]]:
    info = await qgm_fetch_raw(goods_id)
    curr_map = _qgm_build_map(info)
    prev_map = _qgm_prev_status.get(goods_id, {})
    change_lines: list[str] = []
    avail_lines: list[str] = []

    for key, status in curr_map.items():
        date, ticket_type = key.split("||", 1)
        line = f"{ticket_type} {status}"
        if prev_map.get(key) != status:
            change_lines.append(line)
        if status == "预售中":
            avail_lines.append(line)

    return change_lines, avail_lines, curr_map

async def qgm_tick_and_maybe_send():
    global _qgm_last_send, _qgm_last_avail_push, _qgm_next_index, _qgm_req_count
    if _qgm_job_lock.locked():
        return
    async with _qgm_job_lock:
        goods_ids = sorted(CONFIG_QGM_GOODS_IDS)
        if not goods_ids:
            return

        if _qgm_next_index >= len(goods_ids):
            _qgm_next_index = 0
        goods_id = goods_ids[_qgm_next_index % len(goods_ids)]
        _qgm_next_index = (_qgm_next_index + 1) % len(goods_ids)

        chg, avail, curr = await qgm_fetch_once_for(goods_id)
        now = time.time()

        should_send = False
        if chg and (now - _qgm_last_send) >= SEND_INTERVAL_CHANGE_SEC:
            should_send = True
        elif (not chg) and avail and (now - _qgm_last_avail_push) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC \
                and (now - _qgm_last_send) >= 0.1:
            should_send = True
            _qgm_last_avail_push = now

        if should_send:
            merged = list(dict.fromkeys([f"{line}" for line in (chg + avail)]))
            body = [
                f"奇古米 goods_id={goods_id} 状态更新：",
                *merged,
                datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
            ]
            await safe_broadcast("\n".join(body), _get_push_groups("qgm", goods_id))
            _qgm_last_send = now
            _qgm_prev_status[goods_id] = curr

    if _qgm_req_count >= REBUILD_THRESHOLD:
        await _rebuild_qgm_client()
        _qgm_req_count = 0

# ===================== 广播发送 =====================
async def safe_broadcast(text: str, target_groups: Optional[Set[int]] = None):
    groups = set(target_groups if target_groups is not None else ENABLED_GROUPS) & set(ENABLED_GROUPS)
    if not text or not groups:
        return

    bots = list(get_bots().values())
    if not bots:
        logger.debug("当前无活跃 Bot，跳过发送。")
        return

    for bot in bots:
        for gid in sorted(groups):
            try:
                await bot.send_group_msg(group_id=gid, message=Message(text))
            except ActionFailed as af:
                logger.warning(f"发送到群 {gid} 失败：{af}")
            except asyncio.TimeoutError:
                logger.warning(f"发送到群 {gid} 超时。")
            except Exception as e:
                logger.warning(f"发送到群 {gid} 异常：{e}")

# ===================== 快照指令 =====================
snapshot_cmd = on_command("票务快照", permission=SUPERUSER, priority=10, block=True)

async def cpp_full_snapshot_lines(group_id: Optional[int] = None) -> list[str]:
    lines: list[str] = []
    for event_id in sorted(CONFIG_CPP_EVENT_IDS):
        if group_id is not None and group_id not in _get_push_groups("cpp", event_id):
            continue
        data = await cpp_fetch_raw(event_id)
        for t in data.get("ticketTypeList", []) or []:
            name = (t.get("ticketName") or "").strip()
            try:
                rem = int(t.get("remainderNum", 0) or 0)
            except Exception:
                rem = 0
            if rem < 0:
                rem = 0
            try:
                open_timer = int(t.get("openTimer", 0) or 0)
            except Exception:
                open_timer = 0
            if open_timer > 0:
                status_label = f"未开售({rem})"
            elif rem > 0:
                status_label = f"可购买({rem})"
            else:
                status_label = "已售罄(0)"
            lines.append(f"{name} {status_label}")
    return lines

async def bili_full_snapshot_lines(group_id: Optional[int] = None) -> list[str]:
    lines: list[str] = []
    project_ids = [pid for pid in sorted(CONFIG_BILI_PROJECT_IDS) if group_id is None or group_id in _get_push_groups("bilibili", pid)]
    single_project = len(project_ids) == 1
    for project_id in project_ids:
        info = await bili_fetch_raw(_build_bili_params(project_id))
        data = info.get("data", {}) or {}
        for screen in data.get("screen_list", []) or []:
            for ticket in screen.get("ticket_list", []) or []:
                date = (ticket.get("screen_name") or screen.get("name") or "").strip()
                desc = (ticket.get("desc") or "").strip()
                sale_flag = (ticket.get("sale_flag") or {})
                display = sale_flag.get("display_name", "") or ""
                count = ticket.get("num", 0)
                try:
                    count = int(count)
                except Exception:
                    count = 0
                if count < 0:
                    count = 0
                line = f"{date} {desc} {display}({count})"
                lines.append(line if single_project else f"[{project_id}] {line}")
    return lines

async def xm_full_snapshot_lines(group_id: Optional[int] = None) -> list[str]:
    lines: list[str] = []
    for goods_id in sorted(CONFIG_XM_FIXED_META.keys()):
        if group_id is not None and group_id not in _get_push_groups("xm", goods_id):
            continue
        info = await xm_fetch_raw(goods_id)
        curr = _xm_build_lines(info, goods_id=goods_id)
        for key, status in curr.items():
            date, ticket_type = key.split("||", 1)
            lines.append(f"{date} {ticket_type} {status}")
    return lines

async def qgm_full_snapshot_lines(group_id: Optional[int] = None) -> list[str]:
    lines: list[str] = []
    for goods_id in sorted(CONFIG_QGM_GOODS_IDS):
        if group_id is not None and group_id not in _get_push_groups("qgm", goods_id):
            continue
        info = await qgm_fetch_raw(goods_id)
        curr = _qgm_build_map(info)
        for key, status in curr.items():
            date, ticket_type = key.split("||", 1)
            lines.append(f"{ticket_type} {status}")
    return lines

@snapshot_cmd.handle()
async def _handle_snapshot(bot: Bot, event: GroupMessageEvent):
    gid = event.group_id
    cpp_lines = await cpp_full_snapshot_lines(gid)
    bili_lines = await bili_full_snapshot_lines(gid)
    xm_lines = await xm_full_snapshot_lines(gid)
    qgm_lines = await qgm_full_snapshot_lines(gid)

    group_cpp_ids = [x for x in sorted(CONFIG_CPP_EVENT_IDS) if gid in _get_push_groups("cpp", x)]
    group_bili_ids = [x for x in sorted(CONFIG_BILI_PROJECT_IDS) if gid in _get_push_groups("bilibili", x)]
    group_xm_ids = [x for x in sorted(CONFIG_XM_FIXED_META.keys()) if gid in _get_push_groups("xm", x)]
    group_qgm_ids = [x for x in sorted(CONFIG_QGM_GOODS_IDS) if gid in _get_push_groups("qgm", x)]

    title_cpp = "、".join(group_cpp_ids)
    title_bili = "、".join(str(x) for x in group_bili_ids)
    title_xm = "、".join(str(x) for x in group_xm_ids)
    title_qgm = "、".join(str(x) for x in group_qgm_ids)

    body_lines: list[str] = []

    if group_cpp_ids:
        body_lines.append(f"CPP项目{title_cpp}全量：")
        body_lines.extend(cpp_lines or ["（无数据）"])

    if group_bili_ids:
        if body_lines:
            body_lines.append("")
        body_lines.append(f"B站项目{title_bili}全量：")
        body_lines.extend(bili_lines or ["（无数据）"])

    if group_xm_ids:
        if body_lines:
            body_lines.append("")
        body_lines.append(f"小芒项目{title_xm}全量：")
        body_lines.extend(xm_lines or ["（无数据）"])

    if group_qgm_ids:
        if body_lines:
            body_lines.append("")
        body_lines.append(f"奇古米项目{title_qgm}全量：")
        body_lines.extend(qgm_lines or ["（无数据）"])

    if body_lines:
        body_lines.append("")
    body_lines.append("当前监控配置：")
    body_lines.append(_format_monitor_summary_for_group(gid, include_empty=False))
    body_lines.append(datetime.now().strftime("%Y.%m.%d %H:%M:%S"))

    try:
        await bot.send_group_msg(group_id=event.group_id, message=Message("\n".join(body_lines)))
    except Exception as e:
        logger.warning(f"发送快照到群 {event.group_id} 失败：{e}")

# ===================== 启用 / 关闭 指令 =====================
enable_cmd = on_command("票务监控启用", permission=SUPERUSER, priority=10, block=True)
disable_cmd = on_command("票务监控关闭", permission=SUPERUSER, priority=10, block=True)

@enable_cmd.handle()
async def _handle_enable(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    gid = event.group_id
    args_text = args.extract_plain_text().strip()

    # 兼容旧行为：无参数仅启用本群推送
    if not args_text:
        if gid not in ENABLED_GROUPS:
            ENABLED_GROUPS.add(gid)
            await _redis_add_enabled_group(gid)
        await _add_group_to_all_existing_configs(gid)
        await enable_cmd.finish(
            f"已启用本群({gid})票务推送。当前启用群：{sorted(list(ENABLED_GROUPS))}\n"
            f"当前监控配置：\n{_format_monitor_summary_for_group(gid)}"
        )

    try:
        parts = shlex.split(args_text)
    except Exception as e:
        await enable_cmd.finish(f"参数解析失败：{e}")

    if len(parts) < 2:
        await enable_cmd.finish(
            "参数不足。\n"
            "示例：\n"
            "/票务监控启用 bilibili 115413\n"
            "/票务监控启用 cpp 5476\n"
            '/票务监控启用 xm 262340 "12月27日 普通票"\n'
            "/票务监控启用 qgm 10877"
        )

    platform = parts[0]
    raw_id = parts[1]
    extra = parts[2] if len(parts) >= 3 else None

    try:
        p = _normalize_platform_name(platform)

        if p == "cpp":
            target_id: int | str = str(raw_id).strip()
            changed = await _add_monitor_config(p, target_id)
        elif p == "bilibili":
            target_id = int(raw_id)
            changed = await _add_monitor_config(p, target_id)
        elif p == "qgm":
            target_id = int(raw_id)
            changed = await _add_monitor_config(p, target_id)
        else:
            target_id = int(raw_id)
            changed = await _add_monitor_config(p, target_id, extra=extra)

        group_msg = ""
        if gid not in ENABLED_GROUPS:
            ENABLED_GROUPS.add(gid)
            await _redis_add_enabled_group(gid)
            group_msg = f"\n本群({gid})已加入推送群"
        subscribed_before = gid in _get_push_groups(p, target_id)
        await _add_config_push_group(p, target_id, gid)

        if changed:
            action = "已新增"
        elif subscribed_before:
            action = "已存在（本次仅确保任务运行）"
        else:
            action = "已存在（本次已加入本群推送）"
        msg = (
            f"{action}监控配置：platform={p} id={target_id}"
            f"{group_msg}\n"
            f"当前监控配置：\n{_format_monitor_summary_for_group(gid)}"
        )

    except Exception as e:
        msg = f"启用失败: {type(e).__name__}: {e}"

    await enable_cmd.finish(msg)

@disable_cmd.handle()
async def _(event: GroupMessageEvent, args: Message = CommandArg()):
    gid = event.group_id
    text = args.extract_plain_text().strip()

    try:
        # 1. 无参数：关闭当前群推送
        if not text:
            if gid in ENABLED_GROUPS:
                ENABLED_GROUPS.remove(gid)
                await _redis_remove_enabled_group(gid)
                await _remove_group_from_all_config_buckets(gid)
                msg = f"本群({gid})已关闭票务推送。"
            else:
                msg = f"本群({gid})当前未启用票务推送。"
        else:
            # 2. 有参数：关闭某个平台的某个监控配置
            # 兼容：
            # /票务监控关闭 bilibili 115413
            # /票务监控关闭 cpp 5476
            # /票务监控关闭 xm 262340
            # /票务监控关闭 qgm 10877
            parts = text.split(maxsplit=2)
            if len(parts) < 2:
                raise ValueError("参数格式错误，应为：/票务监控关闭 平台 ID")

            platform = parts[0].strip()
            raw_id = parts[1].strip()

            p = _normalize_platform_name(platform)

            if p == "cpp":
                target_id: int | str = str(raw_id).strip()
            elif p == "bilibili":
                target_id = int(raw_id)
            elif p == "qgm":
                target_id = int(raw_id)
            elif p == "xm":
                target_id = int(raw_id)
            else:
                raise ValueError(f"不支持的平台：{platform}")

            changed = await _remove_config_push_group(p, target_id, gid)
            removed_global = False
            if changed and not _config_has_push_groups(p, target_id):
                removed_global = await _remove_monitor_config(p, target_id)

            action = "已关闭" if changed else "未找到"
            suffix = "（已无推送群，监控任务配置已移除）" if removed_global else ""
            msg = (
                f"{action}监控配置：platform={p} id={target_id}{suffix}\n"
                f"当前监控配置：\n{_format_monitor_summary_for_group(gid)}"
            )

    except Exception as e:
        msg = f"关闭失败: {type(e).__name__}: {e}"

    await disable_cmd.finish(msg)
