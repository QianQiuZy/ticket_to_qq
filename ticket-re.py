from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Set, Tuple

import httpx
from nonebot import get_bots, get_driver, on_command, require
from nonebot.adapters.onebot.v11 import Bot, Message
from nonebot.adapters.onebot.v11.event import GroupMessageEvent
from nonebot.adapters.onebot.v11.exception import ActionFailed
from nonebot.log import logger
from nonebot.permission import SUPERUSER
from nonebot.plugin import PluginMetadata

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler  # noqa: E402

__plugin_meta__ = PluginMetadata(
    name="票务监控（API 扁平模式）",
    description="每 0.8 秒拉取一次扁平 API，比较前后两次响应以判断变动和持续有票。",
    usage="/票务快照 | /票务监控启用 | /票务监控关闭（均仅超级用户）",
    type="application",
    homepage="https://ticket.qianqiuzy.cn",
)

driver = get_driver()

# ===================== 本地发送节流规则 =====================
SEND_INTERVAL_CHANGE_SEC = 3.0          # 有变动：≥3s
CONT_AVAIL_PUSH_MIN_INTERVAL_SEC = 9.0  # 持续有票：≥9s
POLL_API_INTERVAL_SEC = 0.8             # 轮询云端 API 周期

API_TICKET = "https://ticket.qianqiuzy.cn/ticket"

# ===================== 状态缓存与发送控制 =====================
ENABLED_GROUPS: Set[int] = set()
api_client: httpx.AsyncClient | None = None

# 上一轮快照：platform -> {name -> status}
_prev: Dict[str, Dict[int, Dict[str, str]]] = {"cpp": {}, "bili": {}, "xm": {}, "qg": {}}

# 发送节流时间戳
_last_send: Dict[Tuple[str, object], float] = {}
_last_avail_push: Dict[Tuple[str, object], float] = {}

PLAT_NAME = {"cpp": "CPP", "bili": "B站", "xm": "小芒", "qg": "奇古米"}
BUYABLE_SET = {"可购买", "预售中"}  # 持续有票判定

# ===================== 客户端与任务 =====================
@driver.on_startup
async def _init_api_client():
    global api_client
    api_client = httpx.AsyncClient(
        headers={"User-Agent": "TicketBot/1.1-flat"},
        http2=True,
        timeout=httpx.Timeout(connect=2.0, read=5.0, write=2.0, pool=5.0),
    )
    if scheduler.get_job("ticket_watch_flat") is None:
        scheduler.add_job(
            tick_and_maybe_send,
            "interval",
            seconds=POLL_API_INTERVAL_SEC,
            id="ticket_watch_flat",
            max_instances=2,
            coalesce=True,
        )
    logger.info("API 扁平模式任务已启动：每 {} 秒拉取".format(POLL_API_INTERVAL_SEC))

@driver.on_shutdown
async def _close_api_client():
    global api_client
    try:
        if api_client:
            await api_client.aclose()
    finally:
        api_client = None

# ===================== 拉取与对比 =====================
async def _fetch_ticket() -> List[dict]:
    assert api_client is not None
    try:
        r = await api_client.get(API_TICKET, follow_redirects=False)
        return r.json() if r.status_code == 200 else []
    except Exception as e:
        logger.warning(f"拉取 ticket 失败：{e}")
        return []

def _split_by_plat_and_id(payload: List[dict]) -> Dict[str, Dict[int, Dict[str, dict]]]:
    """
    { plat: { activity_id: { name: {status: str, count: Optional[int]} } } }
    """
    res: Dict[str, Dict[int, Dict[str, dict]]] = {"cpp": {}, "bili": {}, "xm": {}, "qg": {}}
    for block in payload or []:
        plat = str(block.get("platform") or "").strip()
        aid = block.get("activity_id")
        items = block.get("items") or []
        if plat not in res:
            continue
        try:
            aid = int(aid)
        except Exception:
            continue
        m: Dict[str, dict] = {}
        for it in items:
            name = str(it.get("name") or "").strip()
            status = str(it.get("status") or "").strip()
            cnt = it.get("count", None)
            try:
                cnt = int(cnt) if cnt is not None else None
            except Exception:
                cnt = None
            if name:
                m[name] = {"status": status, "count": cnt}
        res[plat][aid] = m
    return res

def _fmt_line(plat: str, name: str, item: dict) -> str:
    status = item.get("status", "")
    cnt = item.get("count", None)
    # 仅 B站/CPP 显示数量
    if plat in ("bili", "cpp"):
        if cnt is None:
            cnt = 0
        return f"{name} {status}({cnt})"
    else:
        return f"{name} {status}"

def _diff(plat: str, prev: Dict[str, dict], now: Dict[str, dict]) -> Tuple[List[str], List[str]]:
    changes: List[str] = []
    avail: List[str] = []
    for name, cur in now.items():
        p = prev.get(name)
        # 只要 status 或 count 任一变化就算“变动”
        if (p is None) or (p.get("status") != cur.get("status")) or (p.get("count") != cur.get("count")):
            changes.append(_fmt_line(plat, name, cur))
        # 可购集合（不看数量）
        if (cur.get("status") or "") in BUYABLE_SET:
            avail.append(_fmt_line(plat, name, cur))
    return changes, avail

def _merge_view(plat_map: Dict[int, Dict[str, dict]]) -> Tuple[Dict[str, dict], List[int]]:
    """
    输入：{ aid: { name: {status,count?} } }
    输出：(merged_map, sorted_aids)
    合并策略：后者覆盖前者（通常不会撞 name）
    """
    merged: Dict[str, dict] = {}
    aids = sorted(plat_map.keys())
    for _, m in plat_map.items():
        merged.update(m)
    return merged, aids

async def _maybe_send(plat: str, aid: int, changes: List[str], avail: List[str]):
    key = (plat, aid)
    last_send = _last_send.get(key, 0.0)
    last_avail = _last_avail_push.get(key, 0.0)
    now = time.time()

    should = False
    if changes and (now - last_send) >= SEND_INTERVAL_CHANGE_SEC:
        should = True
    elif (not changes) and avail and (now - last_avail) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC \
         and (now - last_send) >= 0.1:
        should = True
        _last_avail_push[key] = now

    if should:
        merged = list(dict.fromkeys(changes + avail))
        title = f"{PLAT_NAME.get(plat, plat)} 活动{aid} 状态更新："
        body = [title, *merged, datetime.now().strftime("%Y.%m.%d %H:%M:%S")]
        await safe_broadcast("\n".join(body))
        _last_send[key] = now

async def safe_broadcast(text: str):
    if not text or not ENABLED_GROUPS:
        return
    bots = list(get_bots().values())
    if not bots:
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

async def tick_and_maybe_send():
    global _prev
    payload = await _fetch_ticket()
    curr = _split_by_plat_and_id(payload)

    # --- B站/CPP：逐活动 ID ---
    for plat in ("cpp", "bili"):
        prev_plat = _prev.get(plat, {})
        curr_plat = curr.get(plat, {})
        for aid, now_map in curr_plat.items():
            before = prev_plat.get(aid, {})
            changes, avail = _diff(plat, before, now_map)
            title = f"{PLAT_NAME.get(plat, plat)} 活动{aid} 状态更新："
            await _maybe_send_keyed((plat, aid), title, changes, avail)

    # --- XM/QG：合并所有活动 ID ---
    for plat in ("xm", "qg"):
        prev_plat = _prev.get(plat, {})
        curr_plat = curr.get(plat, {})

        prev_merged, _ = _merge_view(prev_plat)
        curr_merged, aids = _merge_view(curr_plat)

        changes, avail = _diff(plat, prev_merged, curr_merged)
        if aids:
            title = f"{PLAT_NAME.get(plat, plat)} 活动{'、'.join(str(a) for a in aids)} 状态更新："
        else:
            title = f"{PLAT_NAME.get(plat, plat)} 状态更新："
        await _maybe_send_keyed((plat, "merged"), title, changes, avail)

    _prev = curr

async def _maybe_send_keyed(key: Tuple[str, object], title: str,
                            changes: List[str], avail: List[str]):
    """
    发送策略：
    - 有变化且距上次发送 ≥ 3s => 发送
    - 无变化且有可购且距“持续有票”上次发送 ≥ 9s，且与上次任意发送间隔 ≥ 0.1s => 发送
    """
    last_send = _last_send.get(key, 0.0)
    last_avail = _last_avail_push.get(key, 0.0)
    now = time.time()

    should = False
    if changes and (now - last_send) >= SEND_INTERVAL_CHANGE_SEC:
        should = True
    elif (not changes) and avail and (now - last_avail) >= CONT_AVAIL_PUSH_MIN_INTERVAL_SEC \
         and (now - last_send) >= 0.1:
        should = True
        _last_avail_push[key] = now

    if should:
        merged = list(dict.fromkeys(changes + avail))
        body = [title, *merged, datetime.now().strftime("%Y.%m.%d %H:%M:%S")]
        await safe_broadcast("\n".join(body))
        _last_send[key] = now

# ===================== 指令：快照 / 启用 / 关闭 =====================
snapshot_cmd = on_command("票务快照", permission=SUPERUSER, priority=10, block=True)

@snapshot_cmd.handle()
async def _handle_snapshot(bot: Bot, event: GroupMessageEvent):
    payload = await _fetch_ticket()
    curr = _split_by_plat_and_id(payload)

    def _lines_for_plat(plat: str) -> List[str]:
        plat_map = curr.get(plat, {})
        lines: List[str] = []
        if not plat_map:
            lines.append(f"{PLAT_NAME.get(plat, plat)}：（无数据）")
            lines.append("")
            return lines

        # B站/CPP：逐活动 ID 分开发（并带数量）
        if plat in ("bili", "cpp"):
            for aid, mp in plat_map.items():
                lines.append(f"{PLAT_NAME.get(plat, plat)} 活动{aid}全量：")
                if not mp:
                    lines.append("（无数据）")
                else:
                    for name, item in mp.items():
                        lines.append(_fmt_line(plat, name, item))
                lines.append("")
            return lines

        # 小芒/奇古米：合并所有活动 ID 一起显示
        aids = sorted(plat_map.keys())
        title_ids = "、".join(str(a) for a in aids)
        lines.append(f"{PLAT_NAME.get(plat, plat)} 活动{title_ids}全量：")
        merged: Dict[str, dict] = {}
        # 合并策略：后出现的覆盖先前（通常没有重名冲突）
        for _, mp in plat_map.items():
            merged.update(mp)
        if not merged:
            lines.append("（无数据）")
        else:
            for name, item in merged.items():
                # 小芒/奇古米不带数量
                lines.append(f"{name} {item.get('status','')}")
        lines.append("")
        return lines

    lines: List[str] = []
    # 输出顺序与示例一致
    lines += _lines_for_plat("cpp")
    lines += _lines_for_plat("bili")
    lines += _lines_for_plat("xm")
    lines += _lines_for_plat("qg")

    lines.append(datetime.now().strftime("%Y.%m.%d %H:%M:%S"))
    try:
        await bot.send_group_msg(group_id=event.group_id, message=Message("\n".join(lines)))
    except Exception as e:
        logger.warning(f"发送快照失败：{e}")

enable_cmd = on_command("票务监控启用", permission=SUPERUSER, priority=10, block=True)
disable_cmd = on_command("票务监控关闭", permission=SUPERUSER, priority=10, block=True)

@enable_cmd.handle()
async def _handle_enable(bot: Bot, event: GroupMessageEvent):
    gid = event.group_id
    if gid not in ENABLED_GROUPS:
        ENABLED_GROUPS.add(gid)
    await enable_cmd.finish(f"已启用本群({gid})票务推送。当前启用群：{sorted(list(ENABLED_GROUPS))}")

@disable_cmd.handle()
async def _handle_disable(bot: Bot, event: GroupMessageEvent):
    gid = event.group_id
    if gid in ENABLED_GROUPS:
        ENABLED_GROUPS.remove(gid)
        await disable_cmd.finish(f"已关闭本群({gid})票务推送。当前启用群：{sorted(list(ENABLED_GROUPS))}")
    else:
        await disable_cmd.finish("本群未开启监控，无需关闭。")
