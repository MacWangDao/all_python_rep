import datetime

import aioredis
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
import json
import itertools
import traceback
from .api_twap_pov_v3 import TWAP, Order, PoV, ProducerClient
import os
from loguru import logger
from sqlalchemy.orm.session import Session
from app.models.strategy_stock import Orderno_Sessionid, HostInfo
from app.db.session import SessionLocal


class Base(object):
    job_id_counter = itertools.count()
    global_order_info = {}
    login_status = 0
    order_id_counter = itertools.count()
    orderno_list = []
    ssid = {}
    pause_ssid = {}

    query_order = {}
    total_asset = {}
    hold_stock = {}
    success_order = {}
    login_resp = {}
    cancal_order = []


class BaseServer(Base):
    def __init__(self, user, account, password):
        self.user = user
        self.account = account
        self.password = password

    def login_info(self):
        login = {"type": 3, "reqid": 1, "user": self.user,
                 "accountInfo": {"account": self.account, "pwd": self.password, "authid": "2100014710",
                                 "authcode": "cz4nUbrA41352lwF082j7W26OeG3u24a6p7278Fq5N9kJ5ZkJ396og1o5du02944AaXQOfI9eeb3Ty8MH7A34sr3X5Q7CN1k8GNl",
                                 "ip": "119.254.65.58", "port": 32030, "localip": "192.168.101.25",
                                 "mac": "7486E202AD5B",
                                 "pcname": "DESKTOP-3JARBPU", "diskid": "FFFF_FFFF_FFFF_FFFF.",
                                 "cpuid": "BFEBFBFF000A0671",
                                 "pi": "C^NTFS^237G", "vol": "DFD9-18CD", "clientname": "EMS",
                                 "clientversion": "2.0.0.0000"}}

        return login

    def order_accountinfo(self):
        account = self.login_info().get("accountInfo").get("account")
        return {"accountInfo": {"account": account}}


class AsyncKafkaServer(BaseServer):
    def __init__(self, config):
        self.config = config
        self.user = self.config.get("user_info", {}).get("user")
        self.account = self.config.get("user_info", {}).get("account")
        self.password = self.config.get("user_info", {}).get("password")
        super().__init__(self.user, self.account, self.password)
        self.redis_info = self.config.get("redis_info", {})
        self.redis = aioredis.from_url(f"redis://{self.redis_info.get('host')}", encoding="utf-8",
                                       decode_responses=True, db=1)
        self.redis_database = aioredis.from_url(f"redis://{self.redis_info.get('host')}", encoding="utf-8",
                                                decode_responses=True, db=2)
        self.bootstrap_servers = self.config.get("kafka_server", {}).get("bootstrap_servers", [])
        self.command_topic = self.config.get("kafka_server", {}).get("topics", {}).get("command_topic")
        self.req_pipe = self.config.get("kafka_server", {}).get("topics", {}).get("req_pipe")
        self.rsp_pipe = self.config.get("kafka_server", {}).get("topics", {}).get("rsp_pipe")

        self.twap_limit_count = self.config.get("twap_limit_count", 1)
        self.pov_limit_count = self.config.get("pov_limit_count", 1)
        self.pov_limit_sub_count = self.config.get("pov_limit_sub_count", 1)

    @logger.catch
    async def service(self):

        consumer = AIOKafkaConsumer(
            self.command_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id="command-01")
        await consumer.start()

        consumer_query = AIOKafkaConsumer(
            self.rsp_pipe,
            bootstrap_servers=self.bootstrap_servers,
            enable_auto_commit=True,
            group_id="query-order-01")
        await consumer_query.start()

        async def login():
            logger.info(f"login:user:{self.user}")
            start_login = self.login_info()
            asyncio.ensure_future(self.producer_client("user-login", start_login, ptype=1))
            # asyncio.ensure_future(producer.send(self.req_pipe, key="user", value=start_login))

        @logger.catch
        async def consumer_request_async():
            async for request in consumer:
                try:
                    logger.info(
                        f"request consumed:{request.topic},{request.partition},{request.offset},{request.key},{request.value},{request.timestamp}")

                    if self.login_status == 1:
                        job = self.create_job_kafka(request.value)
                        # job.producer = producer
                        job.producer_client = ProducerClient(self.bootstrap_servers, self.req_pipe)
                        job.order_id_counter = self.order_id_counter
                        job.job_id = next(self.job_id_counter)
                        # await self.redis.incr("job_id", amount=1)
                        # job_id = await self.redis.mget("job_id")
                        # job.job_id = int(job_id[0])

                        asyncio.ensure_future(job.job_order_async())

                    else:
                        logger.info("未登录")
                        await asyncio.sleep(1)
                        # os._exit(0)
                except Exception:
                    traceback.print_exc()

        @logger.catch
        async def consumer_response_async():
            async for response in consumer_query:
                try:
                    order_response = json.loads(response.value.decode("utf-8"))
                    # print(order_response)
                    if order_response.get("errorcode"):
                        logger.error(f"errorcode:{order_response.get('errorcode')}")
                    if order_response.get("type") == 4:  # 登陆
                        self.login_resp = order_response
                        if order_response.get("status") == 1:
                            logger.info("login:success")
                        else:
                            logger.info("login:fail")
                            await asyncio.sleep(5)
                        self.login_status = order_response.get("status")
                    elif order_response.get("type") == 6:  # 报单录入
                        order_id = order_response.get("orderid")
                        if order_id is not None:
                            if self.global_order_info.get(order_id):
                                status = order_response.get("status")
                                self.global_order_info[order_id].update(
                                    {"status": status, "type": order_response.get("type"),
                                     "orderno": order_response.get("orderno"),
                                     "sessionid": order_response.get("sessionid")})
                                in_data = {
                                    "userid": self.global_order_info.get(order_id, {}).get("userid"),
                                    "ssid": self.global_order_info.get(order_id, {}).get("ssid"),
                                    "order_id": order_id,
                                    "orderno": order_response.get("orderno"),
                                    "sessionid": order_response.get("sessionid"),
                                    "status": status,
                                }
                                asyncio.ensure_future(self.redis_database.publish("ssid:1", json.dumps(in_data)))
                                in_data["ctime"] = datetime.datetime.now()
                                self.to_database(in_data=in_data)

                    elif order_response.get("type") == 8:  # 撤单
                        self.cancal_order.clear()
                        self.cancal_order.append(order_response)
                    elif order_response.get("type") == 14:  # 报单查询
                        self.query_order.clear()
                        self.query_order = order_response
                        orderInfo = order_response.get("orderInfo", [])
                        for order in orderInfo:
                            orderno = order.get("orderno")
                            if orderno not in self.orderno_list:
                                self.orderno_list.append(orderno)
                                asyncio.ensure_future(self.redis_database.publish("channel:1", json.dumps(order)))
                        if len(self.orderno_list) > 1000:
                            self.orderno_list = []
                        order_id = order_response.get("orderid")
                        if order_id is not None:
                            if self.global_order_info.get(order_id):
                                status = order_response.get("status")
                                self.global_order_info[order_id]["status"] = status
                                self.global_order_info[order_id]["type"] = order_response.get("type")
                                orderInfo = order_response.get("orderInfo", [])
                                for order in orderInfo:
                                    self.global_order_info[order_id].update(
                                        {"orderno": order.get("orderno"), "ordervol": order.get("ordervol"),
                                         "tradevol": order.get("tradevol"),
                                         "orderstatus": order.get("orderstatus")})
                    elif order_response.get("type") == 16:  # 成交查询
                        self.success_order.clear()
                        self.success_order = order_response
                        logger.info(order_response)
                    elif order_response.get("type") == 12:  # 资金查询
                        self.total_asset.clear()
                        self.total_asset = order_response
                        logger.info(order_response)
                    elif order_response.get("type") == 10:
                        self.hold_stock.clear()
                        self.hold_stock = order_response
                        logger.info(order_response)
                except Exception:
                    traceback.print_exc()

        try:
            asyncio.ensure_future(login())
            await asyncio.sleep(0.8)
            asyncio.ensure_future(consumer_request_async())
            await consumer_response_async()

        except Exception:
            traceback.print_exc()
        finally:
            await consumer.stop()
            await consumer_query.stop()

    def run(self):
        asyncio.ensure_future(self.service())

    @logger.catch
    async def producer_client(self, key, value, ptype=1):
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                    key_serializer=lambda k: json.dumps(k).encode(),
                                    value_serializer=lambda v: json.dumps(v).encode())
        await producer.start()
        try:
            if ptype == 1:
                if value is not None:
                    asyncio.ensure_future(producer.send(self.req_pipe, key=key, value=value))
            elif ptype == 2:
                asyncio.ensure_future(producer.send(self.command_topic, key=key, value=value))
        except Exception:
            traceback.print_exc()
        finally:
            await producer.stop()

    @logger.catch
    async def create_job_web(self, record):

        if record is not None:
            order = Order()
            order.volume = record.get("vol")
            order.price = record.get("price")
            order.bsdir = record.get("bsdir")
            order.exchangeid = record.get("exchangeid")
            request_type = record.get("type")
            order.securityid = record.get("stockcode", "")
            limit = record.get("limit")
            if request_type == 1:
                limit_complete = False
                if limit == 1:
                    limit_complete = True
                job = TWAP(order=order, order_cycle=record["period"], step_size=record["step"],
                           cancellation_time=record["cancel"], limit_complete=limit_complete)
                job.limit_count = self.twap_limit_count
                job.send_topic = self.req_pipe
                job.order_info = self.global_order_info
                job.login_info = self.order_accountinfo()
                job.order_id_counter = self.order_id_counter
                await self.redis.incr("job_id", amount=1)
                job_id = await self.redis.mget("job_id")
                job.job_id = int(job_id[0])
                job.redis = self.redis
                return job
            elif request_type == 2:
                bookkeep = False
                if limit == 1:
                    bookkeep = True
                job = PoV(order=order, transaction_rate=record["period"], step_size=record["step"],
                          cancellation_time=record["cancel"], bookkeep=bookkeep)
                job.limit_count = self.pov_limit_count
                job.limit_sub_count = self.pov_limit_sub_count
                job.send_topic = self.req_pipe
                job.order_info = self.global_order_info
                job.login_info = self.order_accountinfo()
                job.order_id_counter = self.order_id_counter
                await self.redis.incr("job_id", amount=1)
                job_id = await self.redis.mget("job_id")
                job.job_id = int(job_id[0])
                job.redis = self.redis
                return job

    @logger.catch
    def create_job_kafka(self, request_msg):
        order = Order()
        request = request_msg.decode("utf-8")
        request = json.loads(request)
        order.volume = request.get("vol")
        order.price = request.get("price", 0)
        if request.get("price") <= 0:
            order.price = None
        order.bsdir = request.get("bsdir")
        order.exchangeid = request.get("exchangeid")
        request_type = request.get("type")
        order.securityid = request.get("stockcode", "")
        limit = request.get("limit")
        ssid = request.get("ssid")
        userid = request.get("userid")
        if request_type == 1:
            limit_complete = False
            if limit == 1:
                limit_complete = True
            job = TWAP(order=order, order_cycle=request["period"], step_size=request["step"],
                       cancellation_time=request["cancel"], limit_complete=limit_complete)
            job.limit_count = self.twap_limit_count
            job.send_topic = self.req_pipe
            job.order_info = self.global_order_info
            job.login_info = self.order_accountinfo()
            job.redis = self.redis
            job.ssid = ssid
            job.userid = userid
            job.pause_ssid = self.pause_ssid.update({ssid: False})
            return job
        elif request_type == 2:
            bookkeep = False
            if limit == 1:
                bookkeep = True
            job = PoV(order=order, transaction_rate=request["period"], step_size=request["step"],
                      cancellation_time=request["cancel"], bookkeep=bookkeep)
            job.limit_count = self.pov_limit_count
            job.limit_sub_count = self.pov_limit_sub_count
            job.send_topic = self.req_pipe
            job.order_info = self.global_order_info
            job.login_info = self.order_accountinfo()
            job.redis = self.redis
            job.userid = userid
            job.ssid = ssid
            job.pause_ssid = self.pause_ssid.update({ssid: False})
            return job

    def to_database(self, db: Session = SessionLocal(), in_data=None):
        if in_data is None:
            in_data = {}
        db_obj = Orderno_Sessionid(**in_data)
        db.add(db_obj)
        db.commit()
        db.flush(db_obj)
