import asyncio
import time
from datetime import datetime
import collections
from collections.abc import MutableSequence
import aiohttp
import requests
import sys

# 此项为python3.10的bug，如其他版本无此bug可自行修复
collections.MutableSequence = MutableSequence

from mirai_core import Bot, Updater
from mirai_core.models.Types import MessageType
from mirai_core.models import Event, Message

# 配置项
# 替换为您的机器人 QQ 号
QQ = 111111
HOST = '127.0.0.1'
# 替换为实际端口号
PORT = 1111
# 替换为您的 verifyKey
AUTH_KEY = 'verifyKey'
# 替换为您的群号
GROUP_ID = 111111
# 固定常量，在有票的情况下
SEND_INTERVAL = 3.0
# CPP API部分
# 修改event_id即可更换活动
EVENT_ID = 4670
# 填写CPP cookie
COOKIES = {
    "JSESSIONID": "",
    "token": ""
}

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
    'origin': 'https://cp.allcpp.cn',
    'referer': 'https://cp.allcpp.cn/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0'
}

session = requests.Session()
session.headers.update(HEADERS)
session.cookies.update(COOKIES)

def sync_get_ticket(event_id):
    url = f"https://www.allcpp.cn/allcpp/ticket/getTicketTypeList.do?eventMainId={event_id}"
    r = session.get(url, allow_redirects=False)
    if r.status_code == 200:
        return r.json()
    return {}

async def get_ticket_info_api(event_id):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, sync_get_ticket, event_id)

async def ticket_polling(bot):
    """
    监控 CPP 票务：
    - 余量(remainderNum)变化 => 推送
    - 只要当前“有票”(任一票种 remainderNum > 0) => 每 SEND_INTERVAL 秒重复推送一次快照
    - 余量为 0 时显示“已售罄”，有票时显示“可购买(n)”
    - 显示格式：{square}{ticketName} {status}
    """
    last_send = 0.0
    first_run = True
    prev_map: dict[str, int] = {}  # 票项唯一键 -> 上次余量

    while True:
        data = await get_ticket_info_api(EVENT_ID)
        curr_map: dict[str, int] = {}
        change_lines: list[str] = []
        available_lines: list[str] = []

        for t in data.get("ticketTypeList", []):
            key = str(t.get("id") or t.get("ticketTypeId") or f"{t.get('ticketName','')}|{t.get('square','')}")
            square = (t.get("square") or "").strip()
            name   = (t.get("ticketName") or "").strip()
            rem    = int(t.get("remainderNum", 0) or 0)

            curr_map[key] = rem
            status = f"可购买({rem})" if rem > 0 else "已售罄"
            line   = f"{square} {name} {status}"

            # 变化触发
            if first_run or prev_map.get(key) != rem:
                change_lines.append(line)

            # “有票”快照
            if rem > 0:
                available_lines.append(line)

        has_ticket = len(available_lines) > 0
        now = time.time()

        # 发送条件：有变更 或 有票；同时满足节流
        if (change_lines or has_ticket) and (now - last_send >= SEND_INTERVAL):
            # 合并：先变更，再当前有票快照（去重保持顺序）
            merged = list(dict.fromkeys(change_lines + available_lines))
            body = ["CPP状态更新："] + merged + [datetime.now().strftime("%Y.%m.%d %H:%M:%S")]
            await safe_send(bot, [Message.Plain("\n".join(body))])

            last_send = now
            first_run = False
            prev_map  = curr_map  # 同步最新余量快照

        # 风控：3 秒一轮
        await asyncio.sleep(0.6)

# B站API部分
BILI_URL = "https://show.bilibili.com/api/ticket/project/getV2"

# 修改id和project_id即可修改活动
# 此处兼容多个项目，例如BW和BML来确保双项目同时可用
BILI_PROJECTS = [
    {"version": 134, "id": 100596, "project_id": 100596, "requestSource": "pc-new"},
    # 继续追加其它项目...
]

# 自行填入
BILI_COOKIES = {
    "SESSDATA": "",
    "bili_ticket": "",
    "DedeUserID": "",
    "DedeUserID__ckMd5": "",
    "sid": ""
}

BILI_HEADERS = {
    'Accept': '*/*',
    'Referer': 'https://show.bilibili.com/platform/detail.html',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

bili_session = requests.Session()
bili_session.headers.update(BILI_HEADERS)
bili_session.cookies.update(BILI_COOKIES)

def sync_get_bili(params: dict):
    # 动态设置 Referer，提升兼容性
    headers = dict(bili_session.headers)
    ref_id = params.get("id") or params.get("project_id")
    if ref_id:
        headers["Referer"] = f"https://show.bilibili.com/platform/detail.html?id={ref_id}"

    resp = bili_session.get(BILI_URL, params=params, allow_redirects=False, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    return {}

async def get_bili_info(params: dict):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, sync_get_bili, params)

def format_bili_messages(data: dict) -> list[str]:
    lines = []
    for screen in data.get("data", {}).get("screen_list", []):
        date = screen.get("name")   # e.g. "2024-10-03 周四"
        for ticket in screen.get("ticket_list", []):
            flag = ticket.get("sale_flag", {}).get("number")
            if flag in (2, 8):
                status = ticket.get("sale_flag", {}).get("display_name")
                cnt    = ticket.get("num", 0)
                desc   = ticket.get("desc", "")
                lines.append(f"{date} {desc} {status}({cnt})")
    return lines

def _is_on_sale(flag) -> bool:
    try:
        return int(flag) == 2
    except Exception:
        return False

async def bili_polling(bot: Bot):
    last_send = 0.0
    first_run = True

    # 为每个项目记录“日期||票种描述 -> 状态文案”的映射
    prev_status: dict[int, dict[str, str]] = { (p["project_id"]): {} for p in BILI_PROJECTS }

    while True:
        for params in BILI_PROJECTS:
            pid = params["project_id"]
            info = await get_bili_info(params)
            data = info.get("data", {}) or {}

            curr_status: dict[str, str] = {}
            change_lines: list[str] = []
            available_lines: list[str] = []

            for screen in data.get("screen_list", []):
                for ticket in screen.get("ticket_list", []):
                    # 日期优先取票项上的 screen_name，回退到 screen.name
                    date = (ticket.get("screen_name") or screen.get("name") or "").strip()
                    desc = (ticket.get("desc") or "").strip()

                    sale_flag = (ticket.get("sale_flag") or {})
                    flag = sale_flag.get("number") or ticket.get("sale_flag_number")
                    display = sale_flag.get("display_name", "") or ""

                    # 任何状态都附带数量；无效/缺失则按 0 处理
                    count = ticket.get("num", 0)
                    try:
                        count = int(count)
                    except Exception:
                        count = 0
                    if count < 0:
                        count = 0

                    display_fmt = f"{display}({count})"
                    key = f"{date}||{desc}"

                    # 变更判定仍只跟踪 display（不把数量纳入变更）
                    curr_status[key] = display
                    if first_run or prev_status[pid].get(key) != display:
                        change_lines.append(f"{date} {desc} {display_fmt}")

                    # 有票时进入“定频推送”快照
                    if _is_on_sale(flag):
                        available_lines.append(f"{date} {desc} {display_fmt}")

            has_ticket = len(available_lines) > 0
            now = time.time()

            if (change_lines or has_ticket) and (now - last_send >= SEND_INTERVAL):
                merged = list(dict.fromkeys(change_lines + available_lines))
                body = [f"B站项目{pid}状态更新："] + merged + [datetime.now().strftime("%Y.%m.%d %H:%M:%S")]
                await safe_send(bot, [Message.Plain("\n".join(body))])

                last_send = now
                first_run = False
                prev_status[pid] = curr_status.copy()

            # 项目间休眠，降低抓取频率与抖动
            await asyncio.sleep(0.6)

async def safe_send(bot: Bot, chain):
    try:
        await bot.send_message(
            target=GROUP_ID,
            message_type=MessageType.GROUP,
            message=chain
        )
    except asyncio.TimeoutError:
        print("→ [Warning] send_message 超时，消息可能已发出，但客户端未收到响应。")

async def main():
    bot = Bot(QQ, HOST, PORT, AUTH_KEY)
    bot.session.timeout = aiohttp.ClientTimeout(total=None)
    updater = Updater(bot)
    updater.loop = asyncio.get_running_loop()

    @updater.add_handler([Event.Message])
    async def on_message(event):
        pass

    print("→ [Main] Starting tasks")
    await asyncio.gather(
        updater.run_task(),
        # 如需禁用某一部分爬取直接注释掉即可
        # CPP部分
        ticket_polling(bot),
        # B站部分
        bili_polling(bot),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        input("\n按任意键退出")
        sys.exit(0)
