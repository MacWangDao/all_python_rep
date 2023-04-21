import json

from fastapi import APIRouter, WebSocket
import asyncio

import async_timeout

import aioredis

router = APIRouter()
redis = aioredis.from_url("redis://192.168.101.205", encoding="utf-8", decode_responses=True)
pubsub = redis.pubsub()

bsdir = {1: "买入", 2: "卖出"}
exchangeid = {1: "沪市", 2: "深市"}
orderstatus = {1: "未报", 2: "待报", 3: "已报", 4: "已报待撤",
               5: "部成待撤", 6: "部撤", 7: "已撤", 8: "部成",
               9: "已成", 10: "废单", 11: "撤废", 12: "已确认待撤", 13: "已确认",
               14: "待确认"}


async def reader(websocket: WebSocket, channel: aioredis.client.PubSub):
    while True:
        try:
            async with async_timeout.timeout(1):
                message = await channel.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    await websocket.send_json({'account': '1780012567', 'bsdir': 1, 'exchangeid': 1,
                                               'orderno': '92', 'orderprice': 10.0, 'orderstatus': 1,
                                               'ordervol': 1000.0, 'pricdemode': 1, 'securityid': '600110',
                                               'sessionid': 0, 'tradeprice': 10.0, 'tradevol': 0.0
                                               })

        except asyncio.TimeoutError:
            pass


def redis_service(websocket: WebSocket):
    asyncio.ensure_future(reader(websocket, pubsub))


# @router.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#
#     await pubsub.psubscribe("channel:*")
#
#     # await pubsub.subscribe("channel:1", "channel:2")
#
#     future = asyncio.ensure_future(reader(websocket, pubsub))
#     # await future
#     # while True:
#     #     data = await websocket.receive_text()
#     #     print(data)
#     #     await websocket.send_text(f"Message text was: {data}")
@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    message_count = 0
    await websocket.accept()
    # await pubsub.psubscribe("channel:*")
    await pubsub.subscribe("channel:1")
    while True:
        try:
            message = await pubsub.get_message(ignore_subscribe_messages=True)
            if message is not None:
                message = json.loads(message.get("data"))
                message["bsdir"] = bsdir.get(message.get("bsdir"))
                message["exchangeid"] = exchangeid.get(message.get("exchangeid"))
                message["orderstatus"] = orderstatus.get(message.get("orderstatus"))
                await websocket.send_json({"data": message})
        except:
            pass
        message_count += 1
        await asyncio.sleep(0.01)
        if message_count == 10:
            break

    # for _ in range(3):
    #     await websocket.send_json({'account': '1780012567', 'bsdir': 1, 'exchangeid': 1,
    #                                'orderno': '92', 'orderprice': 10.0, 'orderstatus': 1,
    #                                'ordervol': 1000.0, 'pricdemode': 1, 'securityid': '600110',
    #                                'sessionid': 0, 'tradeprice': 10.0, 'tradevol': 0.0
    #                                })
