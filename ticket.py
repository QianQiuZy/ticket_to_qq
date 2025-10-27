from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Set, Optional
import re
import httpx
from nonebot import get_bots, get_driver, on_command, require
from nonebot.adapters.onebot.v11 import Bot, Message
from nonebot.adapters.onebot.v11.event import GroupMessageEvent
from nonebot.adapters.onebot.v11.exception import ActionFailed
from nonebot.log import logger
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
    usage="/票务快照 | /票务监控启用 | /票务监控关闭（均仅超级用户）",
    type="application",
    homepage="https://example.com",
)

driver = get_driver()
REBUILD_THRESHOLD = 500

# ===================== 硬编码配置 =====================
# 发送节流
SEND_INTERVAL_CHANGE_SEC = 3.0          # 有“状态变动”时的最小发送间隔
CONT_AVAIL_PUSH_MIN_INTERVAL_SEC = 9.0  # 持续有票（无变动）按时间节流的最小发送间隔

# 轮询周期
POLL_INTERVAL_CPP_SEC  = 3.0            # CPP 轮询周期（按需手动调整以适配风控）
POLL_INTERVAL_BILI_SEC = 0.6            # B站轮询周期固定 0.6s
POLL_INTERVAL_XM_SEC   = 0.6            # 小芒轮询：交替 goods_id
POLL_INTERVAL_QG_SEC   = 0.6            # 奇古米轮询：交替 goods_id

_cpp_req_count = 0
_bili_req_count = 0
_xm_req_count = 0
_qg_req_count = 0

_cpp_rebuild_lock = asyncio.Lock()
_bili_rebuild_lock = asyncio.Lock()
_xm_rebuild_lock = asyncio.Lock()
_qg_rebuild_lock = asyncio.Lock()

# —— CPP ——（按需手动填写）
CPP_EVENT_ID   = 5020
CPP_JSESSIONID = "000"
CPP_TOKEN      = "000"

# —— B站 Cookie ——（按需手动填写）
BILI_SESSDATA             = "000"
BILI_BILI_TICKET          = "000"
BILI_DEDEUSERID           = "000"
BILI_DEDEUSERID_CKMD5     = "000"
BILI_SID                  = "000"

# —— B站项目列表（手动维护；可多项目）——
BILI_PROJECTS: List[dict] = [
    {"version": 134, "id": 108406, "project_id": 108406, "requestSource": "pc-new"},
]

# —— 小芒 ——（按需手动填写；两个 goods_id 交替轮询）
XM_GOODS_IDS: List[int] = [256987, 256988]
XM_API = "https://mgecom.api.mgtv.com/goods/dsl/dynamic"
XM_HEADERS = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/135 Safari/537.36",
}

# —— 奇古米 ——（按需手动填写；可多 goods_id 交替轮询）
QG_GOODS_IDS: List[int] = [10223]
QG_API = "https://app.qigumi.com/api/v3/goods/chooseTicketGoodsVenue"
QG_HEADERS = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/135 Safari/537.36",
}

# —— Redis（可选）——
REDIS_URL: Optional[str] = None      # 例如 "redis://:password@127.0.0.1:6379/0"
REDIS_PREFIX: str = "ticket_watch"

# ===================== 工具函数 =====================
def _short_date_cn(s: str) -> str:
    m = re.match(r'^\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*$', s or '')
    if m:
        return f"{int(m.group(2))}月{int(m.group(3))}日"
    return s or ''

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

# ===================== 启用群集合（内存 + Redis 持久化） =====================
ENABLED_GROUPS: Set[int] = set()
_redis: "aioredis.Redis | None" = None

def _redis_key_enabled() -> str:
    return f"{REDIS_PREFIX}:enabled_groups"

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
        logger.warning(f"写入 Redis 失败（添加）：{e}")

async def _redis_remove_enabled_group(gid: int):
    if not _redis:
        return
    try:
        await _redis.srem(_redis_key_enabled(), gid)
    except Exception as e:
        logger.warning(f"写入 Redis 失败（移除）：{e}")

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
            headers=CPP_HEADERS, cookies=cpp_cookies, http2=False,
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
            headers=BILI_HEADERS_BASE, cookies=bili_cookies, http2=True,   # 保持 HTTP/2
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
            headers=XM_HEADERS, http2=True,   # 保持 HTTP/2
            timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
        )

async def _rebuild_qg_client():
    global qg_client
    async with _qg_rebuild_lock:
        try:
            if qg_client:
                await qg_client.aclose()
        except Exception:
            pass
        qg_client = httpx.AsyncClient(
            headers=QG_HEADERS, http2=True,   # 保持 HTTP/2
            timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
        )

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

cpp_client: httpx.AsyncClient | None = None
bili_client: httpx.AsyncClient | None = None
xm_client: httpx.AsyncClient | None = None
qg_client: httpx.AsyncClient | None = None

_cpp_job_lock = asyncio.Lock()
_bili_job_lock = asyncio.Lock()
_xm_job_lock = asyncio.Lock()
_qg_job_lock = asyncio.Lock()

@driver.on_startup
async def _init_clients_and_redis():
    """初始化各平台 HTTP 客户端与 Redis；恢复启用群。"""
    global cpp_client, bili_client, xm_client, qg_client, _redis

    # CPP Cookies
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

    # B站 Cookies
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

    # 小芒：无需 Cookie
    xm_client = httpx.AsyncClient(
        headers=XM_HEADERS,
        http2=True,
        timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
    )

    # 奇古米：无需 Cookie
    qg_client = httpx.AsyncClient(
        headers=QG_HEADERS,
        http2=True,
        timeout=httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0),
    )

    # Redis 初始化（可选）
    if REDIS_URL:
        if not aioredis:
            logger.warning("检测到 REDIS_URL，但未安装 redis>=5；将仅使用内存持久化。")
        else:
            try:
                _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
                await _redis.ping()
                logger.info("Redis 已连接，用于启用群持久化。")
                await _redis_load_enabled_groups()
            except Exception as e:
                _redis = None
                logger.warning(f"Redis 连接失败，将仅使用内存：{e}")

    logger.opt(colors=True).info("<g>nonebot_plugin_ticket_watch</g> 初始化完成")

@driver.on_shutdown
async def _close_clients_and_redis():
    global cpp_client, bili_client, xm_client, qg_client, _redis
    try:
        if cpp_client:
            await cpp_client.aclose()
        if bili_client:
            await bili_client.aclose()
        if xm_client:
            await xm_client.aclose()
        if qg_client:
            await qg_client.aclose()
    finally:
        cpp_client = None
        bili_client = None
        xm_client = None
        qg_client = None
    try:
        if _redis:
            await _redis.close()
    except Exception:
        pass
    finally:
        _redis = None


# ===================== CPP 轮询逻辑 =====================
CPP_URL = "https://www.allcpp.cn/allcpp/ticket/getTicketTypeList.do"

_cpp_prev_map: Dict[str, str] = {}
_cpp_first_run = True
_cpp_last_send = 0.0                 # 上次任意发送时间
_cpp_last_avail_push = 0.0           # 上次“持续有票”推送时间

async def cpp_fetch_raw() -> dict:
    global _cpp_req_count
    _cpp_req_count += 1
    assert cpp_client is not None
    params = {"eventMainId": str(CPP_EVENT_ID)}
    try:
        r = await cpp_client.get(CPP_URL, params=params, follow_redirects=False)
        if r.status_code != 200:
            logger.warning(f"响应异常 status={r.status_code} text={r.text[:200]}")
            return {}
        return r.json()
    except Exception as e:
        logger.warning(f"CPP 请求失败: {e}")
        return {}

async def cpp_fetch_once() -> tuple[list[str], list[str], Dict[str, str]]:
    data = await cpp_fetch_raw()
    change_lines: list[str] = []
    available_lines: list[str] = []
    curr_map: Dict[str, str] = {}

    for t in data.get("ticketTypeList", []) or []:
        key = str(t.get("id") or t.get("ticketTypeId") or f"{t.get('ticketName','')}|{t.get('square','')}")
        square = (t.get("square") or "").strip()
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
        line = f"{square} {name} {status_label}"

        if _cpp_first_run or _cpp_prev_map.get(key) != status_label:
            change_lines.append(line)
        if on_sale:
            available_lines.append(line)

    return change_lines, available_lines, curr_map

async def cpp_full_snapshot_lines() -> list[str]:
    data = await cpp_fetch_raw()
    lines: list[str] = []
    for t in data.get("ticketTypeList", []) or []:
        square = (t.get("square") or "").strip()
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
        lines.append(f"{square} {name} {status_label}")
    return lines

async def cpp_tick_and_maybe_send():
    global _cpp_prev_map, _cpp_first_run, _cpp_last_send, _cpp_last_avail_push, _cpp_req_count
    if _cpp_job_lock.locked():
        return
    async with _cpp_job_lock:
        chg, avail, curr_map = await cpp_fetch_once()
        now = time.time()

        should_send = False
        # 1) 有变动：>=3s
        if chg and (now - _cpp_last_send) >= SEND_INTERVAL_CHANGE_SEC:
            should_send = True
        # 2) 持续有票（无变动）：>=9s
        elif (not chg) and avail and (now - _cpp_last_avail_push) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC \
             and (now - _cpp_last_send) >= 0.1:
            should_send = True
            _cpp_last_avail_push = now

        if should_send:
            merged = list(dict.fromkeys(chg + avail))
            body = [
                f"CPP项目{CPP_EVENT_ID}状态更新：",
                *merged,
                datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
            ]
            await safe_broadcast("\n".join(body))
            _cpp_last_send = now
            _cpp_first_run = False
            _cpp_prev_map = curr_map
    if _cpp_req_count >= REBUILD_THRESHOLD:
        await _rebuild_cpp_client()
        _cpp_req_count = 0

# ===================== B站 轮询逻辑 =====================
BILI_API = "https://show.bilibili.com/api/ticket/project/getV2"

_bili_prev_status: Dict[int, Dict[str, str]] = {}
_bili_first_run = True
_bili_last_send = 0.0
_bili_last_avail_push = 0.0

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
        logger.warning(f"B站请求失败: {e}")
        return {}

async def bili_fetch_once_for(params: dict) -> tuple[list[str], list[str], Dict[str, str]]:
    info = await bili_fetch_raw(params)
    data = info.get("data", {}) or {}
    curr_status: Dict[str, str] = {}
    change_lines: list[str] = []
    available_lines: list[str] = []

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
            if _bili_first_run or _bili_prev_status.get(params.get("project_id"), {}).get(key) != display:
                change_lines.append(f"{date} {desc} {display_fmt}")

            if _is_on_sale(flag):
                available_lines.append(f"{date} {desc} {display_fmt}")

    return change_lines, available_lines, curr_status

async def bili_full_snapshot_lines() -> list[str]:
    lines: list[str] = []
    for p in BILI_PROJECTS:
        info = await bili_fetch_raw(p)
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
                lines.append(f"{date} {desc} {display}({count})")
    return lines

async def bili_tick_and_maybe_send():
    global _bili_prev_status, _bili_first_run, _bili_last_send, _bili_last_avail_push, _bili_req_count

    if _bili_job_lock.locked():
        return
    async with _bili_job_lock:
        chg_all: list[str] = []
        avail_all: list[str] = []
        curr_all: Dict[int, Dict[str, str]] = {}

        tasks = [bili_fetch_once_for(p) for p in BILI_PROJECTS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for p, res in zip(BILI_PROJECTS, results):
            pid = p.get("project_id")
            if isinstance(res, Exception) or pid is None:
                continue
            chg, avail, curr = res
            curr_all[pid] = curr
            chg_all.extend(chg)
            avail_all.extend(avail)

        now = time.time()
        has_change = bool(chg_all)
        has_avail = bool(avail_all)

        should_send = False
        if has_change and (now - _bili_last_send) >= SEND_INTERVAL_CHANGE_SEC:
            should_send = True
        elif (not has_change) and has_avail and (now - _bili_last_avail_push) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC \
             and (now - _bili_last_send) >= 0.1:
            should_send = True
            _bili_last_avail_push = now

        if should_send:
            merged = list(dict.fromkeys(chg_all + avail_all))
            title = "、".join(str(p.get("project_id")) for p in BILI_PROJECTS if p.get("project_id"))
            body = [
                f"B站项目{title}状态更新：",
                *merged,
                datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
            ]
            await safe_broadcast("\n".join(body))
            _bili_last_send = now
            _bili_first_run = False
            _bili_prev_status = curr_all
    if _bili_req_count >= REBUILD_THRESHOLD:
        await _rebuild_bili_client()
        _bili_req_count = 0

# ===================== 小芒 轮询逻辑 =====================
_xm_prev_status: Dict[int, Dict[str, str]] = {}  # goods_id -> { "日期||票种": "有货|已售罄" }
_xm_first_run = True
_xm_last_send = 0.0
_xm_last_avail_push = 0.0
_xm_next_index = 0  # 交替轮询索引

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

def _xm_build_lines(info: dict) -> Dict[str, str]:
    origin = (info or {}).get("originData") or {}
    sku_list = origin.get("sku_list") or []
    site_goods = { (g.get("title") or "").strip(): (g.get("sub_title") or "").strip()
                   for g in (origin.get("ticket_info", {}) or {}).get("ticket_site_goods", []) or [] }

    curr_map: Dict[str, str] = {}
    for sku in sku_list:
        date = (sku.get("spec1") or "").strip()
        ticket_type = site_goods.get(date, "").strip()
        status_text = (sku.get("store_count_text") or "").strip()
        status_norm = "预售中" if status_text == "有货" else "已售罄"
        key = f"{date}||{ticket_type}"
        curr_map[key] = status_norm
    return curr_map

async def xm_fetch_once_for(goods_id: int) -> tuple[list[str], list[str], Dict[str, str]]:
    info = await xm_fetch_raw(goods_id)
    curr_map = _xm_build_lines(info)
    prev_map = _xm_prev_status.get(goods_id, {})
    change_lines: list[str] = []
    avail_lines: list[str] = []

    for key, status in curr_map.items():
        date, ticket_type = key.split("||", 1)
        line = f"{date} {ticket_type} {status}"
        if _xm_first_run or prev_map.get(key) != status:
            change_lines.append(line)
        if status == "预售中":
            avail_lines.append(line)

    return change_lines, avail_lines, curr_map

async def xm_tick_and_maybe_send():
    global _xm_prev_status, _xm_first_run, _xm_last_send, _xm_last_avail_push, _xm_next_index, _xm_req_count
    if _xm_job_lock.locked():
        return
    async with _xm_job_lock:
        if not XM_GOODS_IDS:
            return
        goods_id = XM_GOODS_IDS[_xm_next_index % len(XM_GOODS_IDS)]
        _xm_next_index = (_xm_next_index + 1) % len(XM_GOODS_IDS)

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
            merged = list(dict.fromkeys(chg + avail))
            body = [
                f"小芒项目{goods_id}状态更新：",
                *merged,
                datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
            ]
            await safe_broadcast("\n".join(body))
            _xm_last_send = now
            _xm_first_run = False
            _xm_prev_status[goods_id] = curr
    if _xm_req_count >= REBUILD_THRESHOLD:
        await _rebuild_xm_client()
        _xm_req_count = 0

# ===================== 奇古米 轮询逻辑 =====================
_qg_prev_status: Dict[int, Dict[str, str]] = {}  # goods_id -> {"日期||票种": "未开售|已售罄|预售中|已停售"}
_qg_first_run = True
_qg_last_send = 0.0
_qg_last_avail_push = 0.0
_qg_next_index = 0  # 交替轮询索引

def _qg_status_name(code: int) -> str:
    mapping = {1: "未开售", 2: "已售罄", 3: "预售中", 4: "已停售"}
    return mapping.get(int(code or 0), f"状态{code}")

async def qg_fetch_raw(goods_id: int) -> dict:
    global _qg_req_count
    _qg_req_count += 1
    assert qg_client is not None
    try:
        r = await qg_client.get(QG_API, params={"goods_id": str(goods_id)}, follow_redirects=False)
        if r.status_code != 200:
            logger.warning(f"奇古米响应异常 goods_id={goods_id} status={r.status_code} text={r.text[:200]}")
            return {}
        return r.json()
    except Exception as e:
        logger.warning(f"奇古米请求失败 goods_id={goods_id}: {e}")
        return {}

def _qg_build_map(info: dict) -> Dict[str, str]:
    """从奇古米响应构建 curr_map: '日期||票种' -> '状态'（按 button_status 判定）"""
    data = (info or {}).get("b", {}) or {}
    tgd = data.get("ticket_goods_data") or {}
    venue_list = tgd.get("venue_list") or []

    curr_map: Dict[str, str] = {}

    for venue in venue_list:
        date = _short_date_cn((venue.get("venue_show_time") or venue.get("venue_name") or "").strip())
        v_status = _qg_status_name(venue.get("button_status"))

        for sku in venue.get("ticket_sku_list") or []:
            ticket_type = (sku.get("name") or "").strip()  # 如：DAY1普通票
            key = f"{date}||{ticket_type}"
            curr_map[key] = v_status
    return curr_map

async def qg_fetch_once_for(goods_id: int) -> tuple[list[str], list[str], Dict[str, str]]:
    info = await qg_fetch_raw(goods_id)
    curr_map = _qg_build_map(info)
    prev_map = _qg_prev_status.get(goods_id, {})
    change_lines: list[str] = []
    avail_lines: list[str] = []

    for key, status in curr_map.items():
        date, ticket_type = key.split("||", 1)
        line = f"{date} {ticket_type} {status}"
        # 变动判定
        if _qg_first_run or prev_map.get(key) != status:
            change_lines.append(line)
        # 可购（预售中）判定
        if status == "预售中":
            avail_lines.append(line)

    return change_lines, avail_lines, curr_map

async def qg_tick_and_maybe_send():
    global _qg_prev_status, _qg_first_run, _qg_last_send, _qg_last_avail_push, _qg_next_index, _qg_req_count
    if _qg_job_lock.locked():
        return
    async with _qg_job_lock:
        if not QG_GOODS_IDS:
            return
        goods_id = QG_GOODS_IDS[_qg_next_index % len(QG_GOODS_IDS)]
        _qg_next_index = (_qg_next_index + 1) % len(QG_GOODS_IDS)

        chg, avail, curr = await qg_fetch_once_for(goods_id)
        now = time.time()

        should_send = False
        # 1) 有变动：>=3s
        if chg and (now - _qg_last_send) >= SEND_INTERVAL_CHANGE_SEC:
            should_send = True
        # 2) 持续有票（无变动，预售中）：>=9s
        elif (not chg) and avail and (now - _qg_last_avail_push) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC \
             and (now - _qg_last_send) >= 0.1:
            should_send = True
            _qg_last_avail_push = now

        if should_send:
            merged = list(dict.fromkeys(chg + avail))
            body = [
                f"奇古米 goods_id={goods_id} 状态更新：",
                *merged,
                datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
            ]
            await safe_broadcast("\n".join(body))
            _qg_last_send = now
            _qg_first_run = False
            _qg_prev_status[goods_id] = curr
    if _qg_req_count >= REBUILD_THRESHOLD:
        await _rebuild_qg_client()
        _qg_req_count = 0

# ===================== 广播发送 =====================
async def safe_broadcast(text: str):
    if not text or not ENABLED_GROUPS:
        return

    bots = list(get_bots().values())
    if not bots:
        logger.debug("当前无活跃 Bot，跳过发送。")
        return

    for bot in bots:
        for gid in list(ENABLED_GROUPS):
            try:
                await bot.send_group_msg(group_id=gid, message=Message(text))
            except ActionFailed as af:
                logger.warning(f"发送到群 {gid} 失败：{af}")
            except asyncio.TimeoutError:
                logger.warning(f"发送到群 {gid} 超时。")
            except Exception as e:
                logger.warning(f"发送到群 {gid} 异常：{e}")

# ===================== 调度 & 指令 =====================
@driver.on_startup
async def _schedule_jobs():
    # CPP
    if scheduler.get_job("ticket_watch_cpp") is None:
        scheduler.add_job(
            cpp_tick_and_maybe_send,
            "interval",
            seconds=POLL_INTERVAL_CPP_SEC,
            id="ticket_watch_cpp",
            max_instances=2,
            coalesce=True
        )
    # B站
    if scheduler.get_job("ticket_watch_bili") is None:
        scheduler.add_job(
            bili_tick_and_maybe_send,
            "interval",
            seconds=POLL_INTERVAL_BILI_SEC,
            id="ticket_watch_bili",
            max_instances=2,
            coalesce=True
        )
    # 小芒（交替轮询）
    if scheduler.get_job("ticket_watch_xm") is None:
        scheduler.add_job(
            xm_tick_and_maybe_send,
            "interval",
            seconds=POLL_INTERVAL_XM_SEC,
            id="ticket_watch_xm",
            max_instances=2,
            coalesce=True
        )
    # 奇古米（交替轮询）
    if scheduler.get_job("ticket_watch_qg") is None:
        scheduler.add_job(
            qg_tick_and_maybe_send,
            "interval",
            seconds=POLL_INTERVAL_QG_SEC,
            id="ticket_watch_qg",
            max_instances=2,
            coalesce=True
        )

    logger.opt(colors=True).info("<c>票务监控任务</c> 已启动：CPP以 {} 秒为周期轮询".format(POLL_INTERVAL_CPP_SEC))
    logger.opt(colors=True).info("<c>票务监控任务</c> 已启动：B站以 {} 秒为周期轮询".format(POLL_INTERVAL_BILI_SEC))
    logger.opt(colors=True).info("<c>票务监控任务</c> 已启动：小芒以 {} 秒为周期轮询(交替 goods_id)".format(POLL_INTERVAL_XM_SEC))
    logger.opt(colors=True).info("<c>票务监控任务</c> 已启动：奇古米以 {} 秒为周期轮询(交替 goods_id)".format(POLL_INTERVAL_QG_SEC))


# —— 快照：全量列表 ——（仅 SUPERUSER）
snapshot_cmd = on_command("票务快照", permission=SUPERUSER, priority=10, block=True)

async def bili_full_snapshot_lines() -> list[str]:
    lines: list[str] = []
    for p in BILI_PROJECTS:
        info = await bili_fetch_raw(p)
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
                lines.append(f"{date} {desc} {display}({count})")
    return lines

async def cpp_full_snapshot_lines() -> list[str]:
    data = await cpp_fetch_raw()
    lines: list[str] = []
    for t in data.get("ticketTypeList", []) or []:
        square = (t.get("square") or "").strip()
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
        lines.append(f"{square} {name} {status_label}")
    return lines

async def xm_full_snapshot_lines() -> list[str]:
    lines: list[str] = []
    for gid in XM_GOODS_IDS:
        info = await xm_fetch_raw(gid)
        curr = _xm_build_lines(info)
        for key, status in curr.items():
            date, ticket_type = key.split("||", 1)
            lines.append(f"{date} {ticket_type} {status}")
    return lines

async def qg_full_snapshot_lines() -> list[str]:
    lines: list[str] = []
    for gid in QG_GOODS_IDS:
        info = await qg_fetch_raw(gid)
        curr = _qg_build_map(info)
        for key, status in curr.items():
            date, ticket_type = key.split("||", 1)
            lines.append(f"{date} {ticket_type} {status}")
    return lines

@snapshot_cmd.handle()
async def _handle_snapshot(bot: Bot, event: GroupMessageEvent):
    cpp_lines = await cpp_full_snapshot_lines()
    bili_lines = await bili_full_snapshot_lines()
    xm_lines = await xm_full_snapshot_lines()
    qg_lines = await qg_full_snapshot_lines()

    title_bili = "、".join(str(p.get("project_id")) for p in BILI_PROJECTS if p.get("project_id")) or ""
    title_xm = "、".join(str(g) for g in XM_GOODS_IDS) or ""
    title_qg = "、".join(str(g) for g in QG_GOODS_IDS) or ""

    body_lines: list[str] = []
    body_lines.append(f"CPP项目{CPP_EVENT_ID}全量：")
    body_lines.extend(cpp_lines or ["（无数据）"])

    body_lines.append("")
    body_lines.append(f"B站项目{title_bili}全量：")
    body_lines.extend(bili_lines or ["（无数据）"])

    body_lines.append("")
    body_lines.append(f"小芒项目{title_xm}全量：")
    body_lines.extend(xm_lines or ["（无数据）"])

    body_lines.append("")
    body_lines.append(f"奇古米项目{title_qg}全量：")
    body_lines.extend(qg_lines or ["（无数据）"])

    body_lines.append(datetime.now().strftime("%Y.%m.%d %H:%M:%S"))

    try:
        await bot.send_group_msg(group_id=event.group_id, message=Message("\n".join(body_lines)))
    except Exception as e:
        logger.warning(f"发送快照到群 {event.group_id} 失败：{e}")


# —— 启用/关闭监控 ——（仅 SUPERUSER）
enable_cmd = on_command("票务监控启用", permission=SUPERUSER, priority=10, block=True)
disable_cmd = on_command("票务监控关闭", permission=SUPERUSER, priority=10, block=True)

@enable_cmd.handle()
async def _handle_enable(bot: Bot, event: GroupMessageEvent):
    gid = event.group_id
    if gid not in ENABLED_GROUPS:
        ENABLED_GROUPS.add(gid)
        await _redis_add_enabled_group(gid)
    await enable_cmd.finish(f"已启用本群({gid})票务推送。当前启用群：{sorted(list(ENABLED_GROUPS))}")

@disable_cmd.handle()
async def _handle_disable(bot: Bot, event: GroupMessageEvent):
    gid = event.group_id
    if gid in ENABLED_GROUPS:
        ENABLED_GROUPS.remove(gid)
        await _redis_remove_enabled_group(gid)
        await disable_cmd.finish(f"已关闭本群({gid})票务推送。当前启用群：{sorted(list(ENABLED_GROUPS))}")
    else:
        await disable_cmd.finish("本群未开启监控，无需关闭。")
