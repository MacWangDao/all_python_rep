from fastapi import WebSocket
import asyncio

import async_timeout

import aioredis

redis = aioredis.from_url("redis://192.168.101.205", encoding="utf-8", decode_responses=True)
pubsub = redis.pubsub()


async def reader(websocket: WebSocket, channel: aioredis.client.PubSub):
    while True:
        try:
            async with async_timeout.timeout(1):
                message = await channel.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    print(message)
                    await websocket.send_text(f"Message text was: {'data'}")

        except asyncio.TimeoutError:
            pass


def redis_service(websocket: WebSocket):
    asyncio.ensure_future(reader(websocket, pubsub))
