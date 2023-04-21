import base64
import time
from typing import Generator

import aioredis
import pandas as pd
from influxdb_client import InfluxDBClient
from loguru import logger

"""
influxdb:
  token: pFGIYU6RQZ6bfObi0HD3B3KIF4xYq5IesSzvGVkSo675-6BPXk3phh0SUhVhi-_U93OBC1YhT_j-BQvwqNsDpQ==
  org: bjhy
  bucket: quote-snapshot-signal
  url:  http://192.168.101.201:8086
"""

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


def get_data_frame_stream(url=None, token=None, org=None, query=None) -> Generator:
    try:
        with InfluxDBClient(url=url, token=token, org=org, timeout=3_000) as client:
            df_stream_generator = client.query_api().query_data_frame_stream(query, org=org)
            return df_stream_generator
    except Exception as e:
        logger.exception(e)


def inf_merge_data(time, value, instrument):
    mapping = {"trade_date_time": time, "code": instrument, "lastprice": value}
    return mapping


def inf_merge_vol_data(vol, instrument):
    mapping = {"code": instrument, "volume": vol}
    return mapping


async def inf_redis_price():
    try:
        logger.info("inf_redis_price")
        redis = aioredis.from_url(f"redis://{'192.168.101.205'}:{6379}", encoding="utf-8", db=0,
                                  decode_responses=True)
        url = "http://192.168.101.201:8086"
        token = "pFGIYU6RQZ6bfObi0HD3B3KIF4xYq5IesSzvGVkSo675-6BPXk3phh0SUhVhi-_U93OBC1YhT_j-BQvwqNsDpQ=="
        org = "bjhy"
        query = """
                    from(bucket: "quote-snapshot-signal")
                      |> range(start: -10s)
                      |> filter(fn: (r) => r["_measurement"] == "sl_close_price")
                      |> filter(fn: (r) => r["_field"] == "value")
                      |> window(every:inf)
                      |> last()
                      |> duplicate(column:"_stop",as:"_time")
                      |> drop(columns: ["result","_start", "_stop", "table", "_field","_measurement"])
                      |> yield()
                    """
        dfs = get_data_frame_stream(url, token, org, query)
        if dfs:
            for df in dfs:
                logger.info(df.shape)
                df.drop(["result", "table"], axis=1, inplace=True)
                df["_time"] = pd.DatetimeIndex(df["_time"]).tz_convert('Asia/Shanghai')
                df["_time"] = df["_time"].dt.strftime('%Y-%m-%d %H:%M:%S')
                df["instrument"] = df["instrument"].str.split(".", expand=True)[1]
                inf_list = df.to_dict(orient="records")
                for inf in inf_list:
                    time = inf.get("_time")
                    value = inf.get("_value")
                    instrument = inf.get("instrument")
                    mapping = inf_merge_data(time, value, instrument)
                    name = base64.b64encode(mapping.get("code").encode()).decode()
                    await redis.hset(name, mapping=mapping)
    except Exception as e:
        logger.exception(e)
    # await asyncio.sleep(4)
    # await inf_redis_price()


async def inf_redis_vol():
    try:
        logger.info("inf_redis_vol")
        redis = aioredis.from_url(f"redis://{'192.168.101.205'}:{6379}", encoding="utf-8", db=0,
                                  decode_responses=True)
        url = "http://192.168.101.201:8086"
        token = "pFGIYU6RQZ6bfObi0HD3B3KIF4xYq5IesSzvGVkSo675-6BPXk3phh0SUhVhi-_U93OBC1YhT_j-BQvwqNsDpQ=="
        org = "bjhy"
        query = """
                    from(bucket: "quote-snapshot-signal")
                      |> range(start: -10h)
                      |> filter(fn: (r) => r["_measurement"] == "sl_actbuy_vol" or r["_measurement"] == "sl_actsell_vol")
                      |> filter(fn: (r) => r["_field"] == "value")
                      |> window(every:inf)
                      |> last()
                      |> duplicate(column:"_stop",as:"_time")
                      |> drop(columns: ["result","_start", "_stop", "table"])
                      |> group(columns: ["_measurement", "_field"])
                      |> yield()
                    """
        query = """
                        from(bucket: "quote-snapshot-signal")
                          |> range(start: -10s)
                          |> filter(fn: (r) => r["_measurement"] == "sl_actbuy_vol" or r["_measurement"] == "sl_actsell_vol")
                          |> filter(fn: (r) => r["_field"] == "value")
                          |> last()
                          |> duplicate(column:"_stop",as:"_time")
                          |> drop(columns: ["result","_start", "_stop", "table"])
                          |> group(columns: ["instrument"])
                          |> sum()
                          |> yield() 
                        """
        dfs = get_data_frame_stream(url, token, org, query)
        if dfs:
            # pd.set_option('display.max_columns', None)
            # pd.set_option('display.max_rows', None)
            # pd.set_option('max_colwidth', 100)
            for df in dfs:
                logger.info(df.shape)
                df.drop(["result", "table"], axis=1, inplace=True)
                df["instrument"] = df["instrument"].str.split(".", expand=True)[1]
                inf_list = df.to_dict(orient="records")
                for inf in inf_list:
                    vol = inf.get("_value")
                    instrument = inf.get("instrument")
                    mapping = inf_merge_vol_data(vol, instrument)
                    name = base64.b64encode(mapping.get("code").encode()).decode()
                    await redis.hset(name, mapping=mapping)
    except Exception as e:
        logger.exception(e)
    # await asyncio.sleep(5)
    # await inf_redis_vol()


def inf_redis_price_vol():
    for _ in range(15):
        print(22222222)
        time.sleep(3)
        # await asyncio.sleep(3)
    # await inf_redis_price()
    # asyncio.ensure_future(inf_redis_price())
    # asyncio.ensure_future(inf_redis_vol())
    # await inf_redis_vol()
    # await asyncio.gather(inf_redis_price(), inf_redis_vol())


if __name__ == "__main__":
    # asyncio.run(inf_redis_price())
    asyncio.run(inf_redis_vol())
