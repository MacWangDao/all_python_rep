from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
import functools
import json
import itertools
import traceback
import sys
import os
from loguru import logger

"""
1|{"id":"1", "vol":1000, "period":10, "step": 2, "cancel":5,"type":1}
1800048631
1780012567
1|{"id":"1", "vol":500, "period":10000, "step": 2000, "cancel":5000,"type":2,"stockcode":"600570","price":11.28,"bsdir":1,"exchangeid":1,"limit":0}
"""


@logger.catch
async def producer_service(config, command=None):
    command_id_counter = itertools.count()
    bootstrap_servers = config.get("kafka_server", {}).get("bootstrap_servers", [])
    command_topic = config.get("kafka_server", {}).get("topics", {}).get("command_topic")
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers,
                                key_serializer=lambda k: json.dumps(k).encode(),
                                value_serializer=lambda v: json.dumps(v).encode())
    await producer.start()

    try:
        command_id = next(command_id_counter)
        asyncio.ensure_future(producer.send(command_topic, key=command_id, value=command))
        logger.info(command)
    except Exception:
        traceback.print_exc()
    finally:
        await producer.stop()


def service(config, command):
    # command = {"id": "1", "vol": 500, "period": 10000, "step": 2000, "cancel": 5000, "type": 2, "stockcode": "600570",
    #            "price": 11.28, "bsdir": 1, "exchangeid": 1, "limit": 0}
    asyncio.ensure_future(producer_service(config, command=command))
    # asyncio.run(producer_service(config, envs, command=command))


if __name__ == '__main__':
    command = {"id": "1", "vol": 500, "period": 10000, "step": 2000, "cancel": 5000, "type": 2, "stockcode": "600570",
               "price": 11.28, "bsdir": 1, "exchangeid": 1, "limit": 0}
    # service(command)
