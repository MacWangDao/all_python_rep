import asyncio
import traceback
import base64
from concurrent.futures import ProcessPoolExecutor

import aioredis
import zmq
from loguru import logger
from zmq.asyncio import Context
from app.zmq_client import msgcarrier_pb2
from app.zmq_client.zmq_utils import protobuf_snapshot_parse, parse_pb, snapshot_merge_data
import datetime

ctx = Context.instance()
redis_host = "192.168.101.205"
redis_port = 6379
zmq_host = "82.156.87.227"
zmq_port = 8046


async def break_envelope():
    count = 0
    while 1:
        now = datetime.datetime.now()
        await asyncio.sleep(1)
        count += 1
        if count == 10:
            break


class Zmq_Recv:

    def __init__(self, redis_host=None, redis_port=6379, zmq_host=None, zmq_port=8046):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.zmq_host = zmq_host
        self.zmq_port = zmq_port
        self.count = 0

    async def break_envelope(self):
        while 1:
            await asyncio.sleep(1)
            self.count += 1
            if self.count == 10:
                break

    async def async_zmq_recv(self, code_list):
        count = 0
        redis = aioredis.from_url(f"redis://{self.redis_host}:{self.redis_port}", encoding="utf-8", db=0,
                                  decode_responses=True)
        socket = ctx.socket(zmq.SUB)
        socket.connect(f"tcp://{self.zmq_host}:{self.zmq_port}")
        ex = 0
        for code in code_list:
            if code.startswith("6"):
                ex = 1
            elif code.startswith("0") or code.startswith("3"):
                ex = 2
            subscribe_code = f"26.{ex}.{code}"
            socket.setsockopt(zmq.SUBSCRIBE, subscribe_code.encode('utf-8'))
        while True:
            msg_list = await socket.recv_multipart()
            if len(msg_list) == 2:
                try:
                    header_msg = msg_list[0].decode('utf-8')
                    msg_val = header_msg.split(".")
                    if len(msg_val) == 3:
                        msg_type, ex, code = header_msg.split(".")
                        if msg_type == "26":
                            if code.startswith("00") or code.startswith("30") or code.startswith("6"):
                                msg = msgcarrier_pb2.MsgCarrier()
                                msg.ParseFromString(msg_list[1])
                                message_info = msg.message
                                snapshot = protobuf_snapshot_parse(message_info)
                                snapshot_data = parse_pb(snapshot)
                                mapping = snapshot_merge_data(snapshot_data)
                                name = base64.b64encode(mapping.get("code").encode()).decode()
                                await redis.hset(name, mapping=mapping)
                        count += 1
                        if not code_list:
                            break
                        if count == 20 * len(code_list):
                            break
                except Exception as e:
                    traceback.print_exc()
                    break


async def async_zmq_recv(code_list, redis_host=None, redis_port=6379, zmq_host=None, zmq_port=8046):
    count = 0
    redis = aioredis.from_url(f"redis://{redis_host}:{redis_port}", encoding="utf-8", db=0, decode_responses=True)
    socket = ctx.socket(zmq.SUB)
    socket.connect(f"tcp://{zmq_host}:{zmq_port}")
    ex = 0
    for code in code_list:
        if code.startswith("6"):
            ex = 1
        elif code.startswith("0") or code.startswith("3"):
            ex = 2
        subscribe_code = f"26.{ex}.{code}"
        socket.setsockopt(zmq.SUBSCRIBE, subscribe_code.encode('utf-8'))
    while True:
        msg_list = await socket.recv_multipart()
        if len(msg_list) == 2:
            try:
                header_msg = msg_list[0].decode('utf-8')
                msg_val = header_msg.split(".")
                if len(msg_val) == 3:
                    msg_type, ex, code = header_msg.split(".")
                    if msg_type == "26":
                        if code.startswith("00") or code.startswith("30") or code.startswith("6"):
                            msg = msgcarrier_pb2.MsgCarrier()
                            msg.ParseFromString(msg_list[1])
                            message_info = msg.message
                            snapshot = protobuf_snapshot_parse(message_info)
                            snapshot_data = parse_pb(snapshot)
                            mapping = snapshot_merge_data(snapshot_data)
                            logger.info(f"code:{mapping.get('code')},lastprice:{mapping.get('lastprice')}")
                            name = base64.b64encode(mapping.get("code").encode()).decode()
                            await redis.hset(name, mapping=mapping)
                    count += 1
                    if not code_list:
                        break
                    if count == 5 * len(code_list):
                        break
            except Exception as e:
                logger.error(e)
                break


def main(code_list, redis_host=None, redis_port=6379, zmq_host=None, zmq_port=8046):
    asyncio.run(
        async_zmq_recv(code_list, redis_host=redis_host, redis_port=redis_port, zmq_host=zmq_host, zmq_port=zmq_port))


# def sf():
#     redis_host = "192.168.101.205"
#     redis_port = 6379
#     zmq_host = "82.156.87.227"
#     zmq_port = 8046
#     client = Redis(host='192.168.101.205', port=6379, decode_responses=True, db=6)
#     code_set_list = list(client.smembers('code_set'))
#     print(len(code_set_list))
#     data = {"redis_host": "192.168.101.205", "redis_port": 6379, "zmq_host": "82.156.87.227", "zmq_port": 8046}
#     with ProcessPoolExecutor(max_workers=10) as pool:
#         for i in range(0, len(code_set_list), 500):
#             # print(code_set_list[i:i + 500])
#             pool.submit(main, (code_set_list[i:i + 500]), **data)


if __name__ == "__main__":
    pass
