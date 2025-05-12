import asyncio
import time
from datetime import datetime
import collections
from collections.abc import MutableSequence
import aiohttp
import requests

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
    last = 0
    while True:
        data = await get_ticket_info_api(EVENT_ID)
        available = [f"{t['ticketName']} 可购买({t['remainderNum']})"
                     for t in data.get('ticketTypeList', [])
                     if t.get('remainderNum', 0) > 0]
        # 9秒发送一次封装信息
        # 对应3次请求
        if available and time.time() - last >= 9:
            # 此处消息头写死用来节约时间，实际更改即可
            msg = f"CPP\n杭州·COMICUP 31\n" + "\n".join(available) + f"\n{datetime.now():%Y.%m.%d %H:%M:%S}"
            await safe_send(bot, msg)
            last = time.time()
        # CPP目前风控极其严格，目前限制在3秒一次请求，此API和创建订单共享风控，风控时长为半个小时
        await asyncio.sleep(3)

# B站API部分
BILI_URL = "https://show.bilibili.com/api/ticket/project/getV2"

# 修改id和project_id即可修改活动
BILI_PARAMS  = {
    "version": 134,
    "id": 100596,
    "project_id": 100596,
    "requestSource": "pc-new"
}

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
    'Referer': 'https://show.bilibili.com/platform/detail.html?id=101369',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

bili_session = requests.Session()
bili_session.headers.update(BILI_HEADERS)
bili_session.cookies.update(BILI_COOKIES)

def sync_get_bili():
    resp = bili_session.get(BILI_URL, params=BILI_PARAMS, allow_redirects=False)
    if resp.status_code == 200:
        return resp.json()
    return {}

async def get_bili_info():
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, sync_get_bili)

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

async def bili_polling(bot: Bot):
    last_send = 0
    prev_msg  = ""
    while True:
        info = await get_bili_info()
        msgs = format_bili_messages(info)

        if msgs:
            now = time.time()
            # 5秒发送一次封装信息
            if now - last_send >= 5:
                # 此处消息头写死用来节约时间，实际更改即可
                full = [f"会员购\nCOMICUP31同人展"] + msgs + [datetime.now().strftime("%Y.%m.%d %H:%M:%S")]
                text = "\n".join(full)
                if text != prev_msg:
                    # 构造消息链
                    chain = [ Message.Plain(text) ]
                    await safe_send(
                        bot,
                        chain
                    )
                    prev_msg  = text
                    last_send = now
        await asyncio.sleep(0.5)

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
