import base64
import datetime
import time
from typing import Generator

import aioredis
import pandas as pd
from influxdb_client import InfluxDBClient
from loguru import logger

import asyncio
from functools import wraps, partial

import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
inf_loop = asyncio.get_event_loop()


def timer(func=None, interval=60):
    if func is None:
        return partial(timer, interval=interval)

    @wraps(func)
    async def decorated(*args, **kwargs):
        while True:
            await asyncio.sleep(interval)
            await func(*args, **kwargs)

    return inf_loop.create_task(decorated())


def inf_merge_data(time, value, instrument):
    mapping = {"trade_date_time": time, "code": instrument, "lastprice": value}
    return mapping


async def sk_redis_price(instrument, price):
    try:
        logger.info("sk_redis_price")
        redis = aioredis.from_url(f"redis://{'192.168.101.205'}:{6379}", encoding="utf-8", db=0,
                                  decode_responses=True)
        time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        mapping = inf_merge_data(time, price, instrument)
        name = base64.b64encode(mapping.get("code").encode()).decode()
        await redis.hset(name, mapping=mapping)

    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    instrument = "601991"
    price = 2.93
    asyncio.run(sk_redis_price(instrument, price))
