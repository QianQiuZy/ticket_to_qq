from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime
from typing import Dict, Optional, Set

import httpx
from nonebot import get_bots, get_driver, on_command, require
from nonebot.adapters.onebot.v11 import Bot, Message
from nonebot.adapters.onebot.v11.event import GroupMessageEvent
from nonebot.adapters.onebot.v11.exception import ActionFailed
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from nonebot.plugin import PluginMetadata

try:
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler  # noqa: E402

__plugin_meta__ = PluginMetadata(
    name="B站票务监控",
    description="轮询 B 站票务接口，按群和项目隔离推送变化/有票快照；支持 Redis 持久化。",
    usage=(
        "/票务快照\n"
        "/票务状态 项目号\n"
        "/票务监控启用 项目号\n"
        "/票务监控关闭\n"
        "/票务监控关闭 项目号"
    ),
    type="application",
    homepage="https://example.com",
)

driver = get_driver()

REBUILD_THRESHOLD = 500
SEND_INTERVAL_CHANGE_SEC = 3.0
CONT_AVAIL_PUSH_MIN_INTERVAL_SEC = 9.0
POLL_INTERVAL_BILI_SEC = 0.3

BILI_API = "https://mall.bilibili.com/mall-search-items/items_detail/info"
BILI_HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip",
    "accept-language": "zh-CN,zh",
    "origin": "https://mall.bilibili.com",
    "referer": "https://mall.bilibili.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0",
    "content-type": "application/json",
    "cache-control": "no-cache",
}

BILI_SESSDATA = "000"
BILI_BILI_TICKET = "000"
BILI_DEDEUSERID = "000"
BILI_DEDEUSERID_CKMD5 = "000"
BILI_SID = "000"

REDIS_URL: Optional[str] = None
REDIS_PREFIX = "ticket_watch"
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/0"

ENABLED_GROUPS: Set[int] = set()
CONFIG_BILI_PROJECT_IDS: Set[int] = set()
CONFIG_PUSH_GROUPS: Dict[int, Set[int]] = {}

_redis: "aioredis.Redis | None" = None
bili_client: httpx.AsyncClient | None = None

_bili_req_count = 0
_bili_next_index = 0
_bili_last_send: Dict[int, float] = {}
_bili_last_avail_push: Dict[int, float] = {}
_bili_prev_status: Dict[int, Dict[str, str]] = {}
_bili_rebuild_lock = asyncio.Lock()
_bili_job_lock = asyncio.Lock()


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


def _redis_key_enabled() -> str:
    return f"{REDIS_PREFIX}:enabled_groups"


def _redis_key_bili() -> str:
    return f"{REDIS_PREFIX}:config:bilibili"


def _redis_key_config_groups(project_id: int) -> str:
    return f"{REDIS_PREFIX}:config_groups:bilibili:{int(project_id)}"


def _mask(s: Optional[str], keep: int = 6) -> str:
    if not s:
        return "(empty)"
    return s[:keep] + "..." + s[-keep:]


def _bili_cookies() -> dict[str, str]:
    cookies = {}
    if BILI_SESSDATA:
        cookies["SESSDATA"] = BILI_SESSDATA
    if BILI_BILI_TICKET:
        cookies["bili_ticket"] = BILI_BILI_TICKET
    if BILI_DEDEUSERID:
        cookies["DedeUserID"] = BILI_DEDEUSERID
    if BILI_DEDEUSERID_CKMD5:
        cookies["DedeUserID__ckMd5"] = BILI_DEDEUSERID_CKMD5
    if BILI_SID:
        cookies["sid"] = BILI_SID
    return cookies


def _get_push_groups(project_id: int) -> Set[int]:
    groups = CONFIG_PUSH_GROUPS.get(int(project_id))
    if groups is None:
        return set()
    return set(groups) & set(ENABLED_GROUPS)


def _ensure_push_group_bucket(project_id: int) -> Set[int]:
    return CONFIG_PUSH_GROUPS.setdefault(int(project_id), set())


def _format_monitor_summary_for_group(group_id: int, include_empty: bool = False) -> str:
    ids = sorted(pid for pid in CONFIG_BILI_PROJECT_IDS if group_id in _get_push_groups(pid))
    if not ids and not include_empty:
        return "（无）"
    return f"B站: {'、'.join(str(x) for x in ids) or '（无）'}"


def _format_monitor_summary(include_empty: bool = False) -> str:
    if not CONFIG_BILI_PROJECT_IDS and not include_empty:
        return "（无）"
    return f"B站: {'、'.join(str(x) for x in sorted(CONFIG_BILI_PROJECT_IDS)) or '（无）'}"


def _reset_bili_state(project_id: Optional[int] = None):
    global _bili_next_index
    if project_id is None:
        _bili_prev_status.clear()
        _bili_last_send.clear()
        _bili_last_avail_push.clear()
        _bili_next_index = 0
        return
    project_id = int(project_id)
    _bili_prev_status.pop(project_id, None)
    _bili_last_send.pop(project_id, None)
    _bili_last_avail_push.pop(project_id, None)
    _bili_next_index = 0


def _first_present(d: dict, *keys: str, default=None):
    for key in keys:
        if key in d and d.get(key) is not None:
            return d.get(key)
    return default


def _dict_items(value) -> list[dict]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _get_screen_list(data: dict) -> list[dict]:
    if not isinstance(data, dict):
        return []
    return _dict_items(_first_present(data, "screenList", "screen_list", default=[]))


def _get_ticket_list(screen: dict) -> list[dict]:
    if not isinstance(screen, dict):
        return []
    return _dict_items(_first_present(screen, "ticketList", "ticket_list", default=[]))


def _get_sale_flag(source: dict) -> dict:
    if not isinstance(source, dict):
        return {}
    sale_flag = _first_present(source, "saleFlag", "sale_flag", default={})
    return sale_flag if isinstance(sale_flag, dict) else {}


def _get_sale_flag_name(source: dict) -> str:
    sale_flag = _get_sale_flag(source)
    return str(_first_present(sale_flag, "display_name", "displayName", default="") or "").strip()


def _get_sale_flag_number(source: dict):
    return _get_sale_flag(source).get("number")


def _is_on_sale(flag) -> bool:
    try:
        return int(flag) == 2
    except Exception:
        return False


async def _redis_load_enabled_groups():
    global ENABLED_GROUPS
    if not _redis:
        return
    try:
        members = await _redis.smembers(_redis_key_enabled())
        gids = set()
        for member in members or []:
            try:
                gids.add(int(member))
            except Exception:
                continue
        ENABLED_GROUPS |= gids
        if gids:
            logger.info("已从 Redis 加载启用群：%s", sorted(gids))
    except Exception as e:
        logger.warning(f"从 Redis 读取启用群失败：{e}")


async def _redis_add_enabled_group(gid: int):
    if _redis:
        try:
            await _redis.sadd(_redis_key_enabled(), gid)
        except Exception as e:
            logger.warning(f"写入 Redis 失败（添加群）：{e}")


async def _redis_remove_enabled_group(gid: int):
    if _redis:
        try:
            await _redis.srem(_redis_key_enabled(), gid)
        except Exception as e:
            logger.warning(f"写入 Redis 失败（移除群）：{e}")


async def _redis_add_config_group(project_id: int, gid: int):
    if _redis:
        try:
            await _redis.sadd(_redis_key_config_groups(project_id), gid)
        except Exception as e:
            logger.warning(f"写入 Redis 失败（添加配置推送群 project_id={project_id} gid={gid}）：{e}")


async def _redis_remove_config_group(project_id: int, gid: int):
    if _redis:
        try:
            await _redis.srem(_redis_key_config_groups(project_id), gid)
        except Exception as e:
            logger.warning(f"写入 Redis 失败（移除配置推送群 project_id={project_id} gid={gid}）：{e}")


async def _redis_delete_config_groups(project_id: int):
    if _redis:
        try:
            await _redis.delete(_redis_key_config_groups(project_id))
        except Exception as e:
            logger.warning(f"写入 Redis 失败（删除配置推送群 project_id={project_id}）：{e}")


async def _redis_load_monitor_configs():
    global CONFIG_BILI_PROJECT_IDS, CONFIG_PUSH_GROUPS
    if not _redis:
        return
    try:
        members = await _redis.smembers(_redis_key_bili())
        CONFIG_BILI_PROJECT_IDS = set()
        for member in members or []:
            try:
                CONFIG_BILI_PROJECT_IDS.add(int(member))
            except Exception:
                continue

        CONFIG_PUSH_GROUPS = {}
        for project_id in CONFIG_BILI_PROJECT_IDS:
            group_members = await _redis.smembers(_redis_key_config_groups(project_id))
            groups = set()
            for member in group_members or []:
                try:
                    groups.add(int(member))
                except Exception:
                    continue
            CONFIG_PUSH_GROUPS[project_id] = groups or set(ENABLED_GROUPS)

        logger.info("已从 Redis 加载监控配置：\n%s", _format_monitor_summary())
    except Exception as e:
        logger.warning(f"从 Redis 读取监控配置失败：{e}")


async def _redis_add_monitor_config(project_id: int):
    if _redis:
        try:
            await _redis.sadd(_redis_key_bili(), int(project_id))
        except Exception as e:
            logger.warning(f"写入 Redis 失败（添加监控配置 project_id={project_id}）：{e}")


async def _redis_remove_monitor_config(project_id: int):
    if _redis:
        try:
            await _redis.srem(_redis_key_bili(), int(project_id))
        except Exception as e:
            logger.warning(f"写入 Redis 失败（移除监控配置 project_id={project_id}）：{e}")


async def _add_config_push_group(project_id: int, gid: int):
    groups = _ensure_push_group_bucket(project_id)
    if gid not in groups:
        groups.add(gid)
        await _redis_add_config_group(project_id, gid)


async def _remove_config_push_group(project_id: int, gid: int) -> bool:
    groups = CONFIG_PUSH_GROUPS.get(int(project_id))
    if not groups or gid not in groups:
        return False
    groups.remove(gid)
    await _redis_remove_config_group(project_id, gid)
    return True


async def _remove_group_from_all_config_buckets(gid: int):
    for project_id, groups in list(CONFIG_PUSH_GROUPS.items()):
        if gid in groups:
            groups.remove(gid)
            await _redis_remove_config_group(project_id, gid)


async def _add_monitor_config(project_id: int) -> bool:
    project_id = int(project_id)
    if project_id in CONFIG_BILI_PROJECT_IDS:
        await _sync_bili_job()
        return False
    CONFIG_BILI_PROJECT_IDS.add(project_id)
    _reset_bili_state(project_id)
    await _redis_add_monitor_config(project_id)
    await _sync_bili_job()
    return True


async def _remove_monitor_config(project_id: int) -> bool:
    project_id = int(project_id)
    if project_id not in CONFIG_BILI_PROJECT_IDS:
        return False
    CONFIG_BILI_PROJECT_IDS.remove(project_id)
    CONFIG_PUSH_GROUPS.pop(project_id, None)
    _reset_bili_state(project_id)
    await _redis_remove_monitor_config(project_id)
    await _redis_delete_config_groups(project_id)
    await _sync_bili_job()
    return True


async def _rebuild_bili_client():
    global bili_client
    async with _bili_rebuild_lock:
        try:
            if bili_client:
                await bili_client.aclose()
        except Exception:
            pass
        bili_client = httpx.AsyncClient(
            headers=BILI_HEADERS,
            cookies=_bili_cookies(),
            http2=True,
            timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
            trust_env=False,
        )


@driver.on_startup
async def _init_client_and_redis():
    global _redis
    await _rebuild_bili_client()
    logger.info("B站 cookies 已载入: SESSDATA=%s", _mask(BILI_SESSDATA))

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

    await _sync_bili_job()
    logger.opt(colors=True).info("<g>nonebot_plugin_ticket_watch</g> 初始化完成")


@driver.on_shutdown
async def _close_client_and_redis():
    global bili_client, _redis
    try:
        if bili_client:
            await bili_client.aclose()
    finally:
        bili_client = None

    try:
        if _redis:
            await _redis.close()
    except Exception:
        pass
    finally:
        _redis = None


async def _sync_bili_job():
    job = scheduler.get_job("ticket_watch_bili")
    if CONFIG_BILI_PROJECT_IDS:
        if job is None:
            scheduler.add_job(
                bili_tick_and_maybe_send,
                "interval",
                seconds=POLL_INTERVAL_BILI_SEC,
                id="ticket_watch_bili",
                max_instances=2,
                coalesce=True,
            )
            logger.info("B站票务监控任务已启动，轮询周期 %.1fs", POLL_INTERVAL_BILI_SEC)
    elif job is not None:
        scheduler.remove_job("ticket_watch_bili")
        logger.info("B站票务监控任务已停止（当前无配置）")


async def bili_fetch_raw(project_id: int) -> dict:
    global _bili_req_count
    _bili_req_count += 1
    assert bili_client is not None
    payload = {"itemsId": int(project_id), "itemsDetailPageType": 3}
    try:
        response = await bili_client.post(BILI_API, json=payload, follow_redirects=False)
        if response.status_code != 200:
            logger.warning("B站响应异常 project_id=%s status=%s text=%s", project_id, response.status_code, response.text[:200])
            return {}
        return response.json()
    except Exception as e:
        logger.warning(f"B站请求失败 project_id={project_id}: {e}")
        return {}


async def bili_fetch_raw_standalone(project_id: int) -> dict:
    payload = {"itemsId": int(project_id), "itemsDetailPageType": 3}
    try:
        async with httpx.AsyncClient(
            headers=BILI_HEADERS,
            cookies=_bili_cookies(),
            http2=True,
            timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
            trust_env=False,
        ) as client:
            response = await client.post(BILI_API, json=payload, follow_redirects=False)
        if response.status_code != 200:
            logger.warning("B站独立状态查询响应异常 project_id=%s status=%s text=%s", project_id, response.status_code, response.text[:200])
            return {}
        return response.json()
    except Exception as e:
        logger.warning(f"B站独立状态查询失败 project_id={project_id}: {e}")
        return {}


def bili_status_lines_from_info(info: dict) -> list[str]:
    data = (info or {}).get("data") or {}
    lines: list[str] = []

    for screen in _get_screen_list(data):
        date = str(_first_present(screen, "name", "screen_name", "screenName", default="") or "").strip()
        screen_status = _get_sale_flag_name(screen)
        for ticket in _get_ticket_list(screen):
            desc = str(_first_present(ticket, "desc", "ticket_desc", "ticketDesc", default="") or "").strip()
            display = _get_sale_flag_name(ticket) or screen_status or "未知"
            count = _first_present(ticket, "num", "stock", "sale_stock", "saleStock", default=0)
            try:
                count = int(count)
            except Exception:
                count = 0
            if count < 0:
                count = 0
            lines.append(f"{date} {desc} {display}({count})".strip())

    return lines


async def bili_standalone_status_lines(project_id: int) -> list[str]:
    info = await bili_fetch_raw_standalone(project_id)
    return bili_status_lines_from_info(info)


async def bili_fetch_once_for(project_id: int) -> tuple[list[str], list[str], Dict[str, str]]:
    info = await bili_fetch_raw(project_id)
    data = (info or {}).get("data") or {}
    curr_status: Dict[str, str] = {}
    change_lines: list[str] = []
    available_lines: list[str] = []
    prev_map = _bili_prev_status.get(int(project_id), {})

    for screen in _get_screen_list(data):
        date = str(_first_present(screen, "name", "screen_name", "screenName", default="") or "").strip()
        screen_status = _get_sale_flag_name(screen)
        for ticket in _get_ticket_list(screen):
            desc = str(_first_present(ticket, "desc", "ticket_desc", "ticketDesc", default="") or "").strip()
            display = _get_sale_flag_name(ticket) or screen_status
            flag = _get_sale_flag_number(ticket)
            count = _first_present(ticket, "num", "stock", "sale_stock", "saleStock", default=0)
            try:
                count = int(count)
            except Exception:
                count = 0
            if count < 0:
                count = 0

            key = f"{date}||{desc}"
            status_key = f"{display}({count})"
            line = f"{date} {desc} {status_key}".strip()
            curr_status[key] = status_key

            if prev_map.get(key) != status_key:
                change_lines.append(line)
            if _is_on_sale(flag):
                available_lines.append(line)

    return change_lines, available_lines, curr_status


async def bili_tick_and_maybe_send():
    global _bili_next_index, _bili_req_count
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
        last_send = _bili_last_send.get(project_id, 0.0)
        last_avail_push = _bili_last_avail_push.get(project_id, 0.0)
        if chg and (now - last_send) >= SEND_INTERVAL_CHANGE_SEC:
            should_send = True
        elif (not chg) and avail and (now - last_avail_push) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC and (now - last_send) >= 0.1:
            should_send = True
            _bili_last_avail_push[project_id] = now

        if should_send:
            merged = list(dict.fromkeys(chg + avail))
            body = [
                f"B站项目{project_id}状态更新：",
                *merged,
                datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
            ]
            await safe_broadcast("\n".join(body), _get_push_groups(project_id))
            _bili_last_send[project_id] = now
            _bili_prev_status[project_id] = curr

    if _bili_req_count >= REBUILD_THRESHOLD:
        await _rebuild_bili_client()
        _bili_req_count = 0


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


snapshot_cmd = on_command("票务快照", permission=SUPERUSER, priority=10, block=True)
status_cmd = on_command("票务状态", permission=SUPERUSER, priority=10, block=True)


async def bili_full_snapshot_lines(group_id: Optional[int] = None) -> list[str]:
    lines: list[str] = []
    project_ids = [pid for pid in sorted(CONFIG_BILI_PROJECT_IDS) if group_id is None or group_id in _get_push_groups(pid)]
    single_project = len(project_ids) == 1
    for project_id in project_ids:
        chg, avail, curr = await bili_fetch_once_for(project_id)
        _bili_prev_status[project_id] = curr
        project_lines = list(dict.fromkeys(chg + avail))
        lines.extend(project_lines if single_project else [f"[{project_id}] {line}" for line in project_lines])
    return lines


@snapshot_cmd.handle()
async def _handle_snapshot(bot: Bot, event: GroupMessageEvent):
    gid = event.group_id
    lines = await bili_full_snapshot_lines(gid)
    group_project_ids = [pid for pid in sorted(CONFIG_BILI_PROJECT_IDS) if gid in _get_push_groups(pid)]

    body_lines: list[str] = []
    if group_project_ids:
        body_lines.append(f"B站项目{'、'.join(str(x) for x in group_project_ids)}全量：")
        body_lines.extend(lines or ["（无数据）"])

    if body_lines:
        body_lines.append("")
    body_lines.append("当前监控配置：")
    body_lines.append(_format_monitor_summary_for_group(gid))
    body_lines.append(datetime.now().strftime("%Y.%m.%d %H:%M:%S"))

    try:
        await bot.send_group_msg(group_id=gid, message=Message("\n".join(body_lines)))
    except Exception as e:
        logger.warning(f"发送快照到群 {gid} 失败：{e}")


@status_cmd.handle()
async def _handle_status(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    args_text = args.extract_plain_text().strip()
    if not args_text:
        await status_cmd.finish("参数不足。示例：/票务状态 115413")

    try:
        project_id = int(args_text.split()[0])
        lines = await bili_standalone_status_lines(project_id)
        body_lines = [
            f"B站项目{project_id}当前票务状态：",
            *(lines or ["（无数据）"]),
            datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
        ]
        await bot.send_group_msg(group_id=event.group_id, message=Message("\n".join(body_lines)))
    except Exception as e:
        await status_cmd.finish(f"查询失败: {type(e).__name__}: {e}")


enable_cmd = on_command("票务监控启用", permission=SUPERUSER, priority=10, block=True)
disable_cmd = on_command("票务监控关闭", permission=SUPERUSER, priority=10, block=True)


@enable_cmd.handle()
async def _handle_enable(event: GroupMessageEvent, args: Message = CommandArg()):
    gid = event.group_id
    args_text = args.extract_plain_text().strip()
    if not args_text:
        await enable_cmd.finish("参数不足。示例：/票务监控启用 115413")

    try:
        project_id = int(args_text.split()[0])
        changed = await _add_monitor_config(project_id)

        group_msg = ""
        if gid not in ENABLED_GROUPS:
            ENABLED_GROUPS.add(gid)
            await _redis_add_enabled_group(gid)
            group_msg = f"\n本群({gid})已加入推送群"

        subscribed_before = gid in _get_push_groups(project_id)
        await _add_config_push_group(project_id, gid)

        if changed:
            action = "已新增"
        elif subscribed_before:
            action = "已存在（本次仅确保任务运行）"
        else:
            action = "已存在（本次已加入本群推送）"

        msg = (
            f"{action}B站监控配置：项目号={project_id}"
            f"{group_msg}\n"
            f"当前监控配置：\n{_format_monitor_summary_for_group(gid)}"
        )
    except Exception as e:
        msg = f"启用失败: {type(e).__name__}: {e}"

    await enable_cmd.finish(msg)


@disable_cmd.handle()
async def _handle_disable(event: GroupMessageEvent, args: Message = CommandArg()):
    gid = event.group_id
    text = args.extract_plain_text().strip()

    try:
        if not text:
            if gid in ENABLED_GROUPS:
                ENABLED_GROUPS.remove(gid)
                await _redis_remove_enabled_group(gid)
                await _remove_group_from_all_config_buckets(gid)
                msg = f"本群({gid})已关闭票务推送。"
            else:
                msg = f"本群({gid})当前未启用票务推送。"
        else:
            project_id = int(text.split()[0])
            changed = await _remove_config_push_group(project_id, gid)
            removed_global = False
            if changed and not CONFIG_PUSH_GROUPS.get(project_id):
                removed_global = await _remove_monitor_config(project_id)

            action = "已关闭" if changed else "未找到"
            suffix = "（已无推送群，监控任务配置已移除）" if removed_global else ""
            msg = (
                f"{action}B站监控配置：项目号={project_id}{suffix}\n"
                f"当前监控配置：\n{_format_monitor_summary_for_group(gid)}"
            )
    except Exception as e:
        msg = f"关闭失败: {type(e).__name__}: {e}"

    await disable_cmd.finish(msg)
