import datetime
import traceback

import aioredis
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
import json
import itertools
from .api_twap_pov_v4 import TWAP, Order, PoV, ProducerClient
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
    cancel_order = []
    login_resp_list = []
    errorcode_res = {}


class BaseServer(Base):
    def __init__(self, user, account, password):
        self.user = user
        self.account = account
        self.password = password

    def login_infos(self):
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
        account = self.login_infos().get("accountInfo").get("account")
        return {"accountInfo": {"account": account}}


class AsyncKafkaServer(BaseServer):
    def __init__(self, config):
        self.config = config
        # self.user = self.config.get("user_info", {}).get("user")
        # self.account = self.config.get("user_info", {}).get("account")
        # self.password = self.config.get("user_info", {}).get("password")
        # super().__init__(self.user, self.account, self.password)
        self.redis_info = self.config.get("redis_info", {})
        self.redis = aioredis.from_url(f"redis://{self.redis_info.get('host')}", encoding="utf-8",
                                       decode_responses=True, db=0)
        self.redis_9 = aioredis.from_url(f"redis://{self.redis_info.get('host')}", encoding="utf-8",
                                         decode_responses=True, db=9)
        self.redis_10 = aioredis.from_url(f"redis://{self.redis_info.get('host')}", encoding="utf-8",
                                          decode_responses=True, db=10)
        self.redis_database = aioredis.from_url(f"redis://{self.redis_info.get('host')}", encoding="utf-8",
                                                decode_responses=True, db=2)
        self.bootstrap_servers = self.config.get("kafka_server", {}).get("bootstrap_servers", [])
        self.command_topic = self.config.get("kafka_server", {}).get("topics", {}).get("command_topic")
        self.req_pipe = self.config.get("kafka_server", {}).get("topics", {}).get("req_pipe")
        self.rsp_pipe = self.config.get("kafka_server", {}).get("topics", {}).get("rsp_pipe")

        self.twap_limit_count = self.config.get("twap_limit_count", 1)
        self.pov_limit_count = self.config.get("pov_limit_count", 1)
        self.pov_limit_sub_count = self.config.get("pov_limit_sub_count", 1)

    async def service(self):
        try:
            consumer = AIOKafkaConsumer(
                self.command_topic,
                bootstrap_servers=self.bootstrap_servers, retry_backoff_ms=1000,
                group_id="command-02")
            await consumer.start()

            consumer_query = AIOKafkaConsumer(
                self.rsp_pipe,
                bootstrap_servers=self.bootstrap_servers, retry_backoff_ms=1000,
                enable_auto_commit=True,
                group_id="query-order-02")
            await consumer_query.start()

            async def login():
                logger.info(f"login:user:{self.user}")
                start_login = self.login_infos()
                asyncio.ensure_future(self.producer_client("user-login", start_login, ptype=1))

            @logger.catch
            async def consumer_request_async():
                async for request in consumer:
                    try:
                        logger.info(
                            f"request consumed:{request.topic},{request.partition},{request.offset},{request.key},{request.value},{request.timestamp}")
                        job = self.create_job_kafka(request.value)
                        job.producer_client = ProducerClient(self.bootstrap_servers, self.req_pipe)
                        # job.order_id_counter = self.order_id_counter
                        # job.job_id = next(self.job_id_counter)
                        await self.redis.incr("job_id", amount=1)
                        job_id = await self.redis.mget("job_id")
                        job.job_id = int(job_id[0])
                        asyncio.ensure_future(job.job_order_async())
                    except Exception as e:
                        logger.exception(e)

            @logger.catch
            async def consumer_response_async():
                async for response in consumer_query:
                    try:
                        order_response = json.loads(response.value.decode("utf-8"))
                        # logger.info(order_response)
                        if order_response.get("error"):
                            self.errorcode_res.clear()
                            self.errorcode_res["errorcode"] = order_response.get("error")
                            logger.error(f"error:{order_response.get('error')}")
                        if order_response.get("type") == 4:  # 登陆
                            self.login_resp = order_response
                            self.login_resp_list.append(order_response)
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
                                        "job_id": self.global_order_info.get(order_id, {}).get("job_id"),
                                        "batid": self.global_order_info.get(order_id, {}).get("batid"),
                                        "order_id": order_id,
                                        "orderno": order_response.get("orderno"),
                                        "sessionid": order_response.get("sessionid"),
                                        "status": status,
                                        "error_msg": order_response.get("error", ""),
                                    }
                                    orderno = order_response.get("orderno", None)
                                    ctime = datetime.datetime.now()
                                    if orderno is not None:
                                        in_data.update({"ctime": ctime.strftime("%Y-%m-%d %H:%M:%S")})
                                        if self.global_order_info.get(order_id, {}).get("not_cancel"):
                                            in_data.update({"not_cancel": 1})
                                        await self.redis_9.hset(orderno, mapping=in_data)
                                    in_data.update({"ctime": ctime})
                                    in_data.pop("not_cancel", None)
                                    self.to_database(in_data=in_data)
                                    # asyncio.ensure_future(self.redis_database.publish("ssid:1", json.dumps(in_data)))
                                    # in_data["ctime"] = datetime.datetime.now()
                                    # self.to_database(in_data=in_data)

                        elif order_response.get("type") == 8:  # 撤单
                            self.cancel_order.clear()
                            self.cancel_order.append(order_response)
                        elif order_response.get("type") == 14:  # 报单查询
                            self.query_order.clear()
                            self.query_order = order_response
                            # orderInfo = order_response.get("orderInfo", [])
                            # for order in orderInfo:
                            #     orderno = order.get("orderno")
                            #     if orderno not in self.orderno_list:
                            #         self.orderno_list.append(orderno)
                            #         asyncio.ensure_future(self.redis_database.publish("channel:1", json.dumps(order)))
                            # if len(self.orderno_list) > 1000:
                            #     self.orderno_list = []
                            order_id = order_response.get("orderid")
                            if order_id is not None:
                                if self.global_order_info.get(order_id):
                                    status = order_response.get("status")
                                    self.global_order_info[order_id]["status"] = status
                                    self.global_order_info[order_id]["type"] = order_response.get("type")
                                    orderInfo = order_response.get("orderInfo", [])
                                    for order in orderInfo:
                                        orderno = order.get("orderno")
                                        mapping = {"orderno": order.get("orderno"), "ordervol": order.get("ordervol"),
                                                   "tradevol": order.get("tradevol"),
                                                   "orderstatus": order.get("orderstatus"), "to_db": 0}
                                        self.global_order_info[order_id].update(mapping)
                                        asyncio.ensure_future(
                                            self.redis_10.publish("redis_oracle:1", json.dumps(order)))
                                        if orderno is not None:
                                            await self.redis_9.hset(orderno, mapping=mapping)
                            else:
                                orderInfo = order_response.get("orderInfo", [])
                                for order in orderInfo:
                                    orderno = order.get("orderno")
                                    mapping = {"orderno": order.get("orderno"), "ordervol": order.get("ordervol"),
                                               "tradevol": order.get("tradevol"),
                                               "orderstatus": order.get("orderstatus"), "to_db": 1}
                                    asyncio.ensure_future(
                                        self.redis_10.publish("redis_oracle:1", json.dumps(order)))
                                    if orderno is not None:
                                        await self.redis_9.hset(orderno, mapping=mapping)

                        elif order_response.get("type") == 16:  # 成交查询
                            self.success_order.clear()
                            self.success_order = order_response
                            # logger.info(order_response)
                        elif order_response.get("type") == 12:  # 资金查询
                            self.total_asset.clear()
                            self.total_asset = order_response
                            logger.info(order_response)
                        elif order_response.get("type") == 10:
                            self.hold_stock.clear()
                            self.hold_stock = order_response
                            # logger.info(order_response)
                    except Exception as e:
                        logger.exception(e)

            try:
                # asyncio.ensure_future(login())
                # await asyncio.sleep(0.8)
                asyncio.ensure_future(consumer_request_async())
                await consumer_response_async()

            except Exception as e:
                logger.exception(e)
                await consumer.stop()
                await consumer_query.stop()
            finally:
                await consumer.stop()
                await consumer_query.stop()
        except Exception as e:
            logger.exception(e)

    def run(self):
        asyncio.ensure_future(self.service())

    @logger.catch
    async def producer_client(self, key, value, ptype=1):
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, retry_backoff_ms=1000,
                                    key_serializer=lambda k: json.dumps(k).encode(),
                                    value_serializer=lambda v: json.dumps(v).encode())
        try:
            await producer.start()
            if ptype == 1:
                if value is not None:
                    asyncio.ensure_future(producer.send(self.req_pipe, key=key, value=value))
            elif ptype == 2:
                asyncio.ensure_future(producer.send(self.command_topic, key=key, value=value))
        except Exception as e:
            logger.exception(e)
            await producer.stop()
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
                job.login_info = {"accountInfo": {"account": self.account}}
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
                job.login_info = {"accountInfo": {"account": self.account}}
                job.order_id_counter = self.order_id_counter
                await self.redis.incr("job_id", amount=1)
                job_id = await self.redis.mget("job_id")
                job.job_id = int(job_id[0])
                job.redis = self.redis
                return job

    @logger.catch
    def create_job_kafka(self, request_msg):
        try:
            order = Order()
            request = request_msg.decode("utf-8")
            request = json.loads(request)
            batid = request.get("batid")
            self.account = request.get("account")
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
                job.login_info = {"accountInfo": {"account": self.account}}
                job.redis = self.redis
                job.ssid = ssid
                job.userid = userid
                job.batid = batid
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
                job.login_info = {"accountInfo": {"account": self.account}}
                job.redis = self.redis
                job.userid = userid
                job.ssid = ssid
                job.batid = batid
                job.pause_ssid = self.pause_ssid.update({ssid: False})
                return job
        except Exception as e:
            logger.exception(e)

    def to_database(self, db: Session = SessionLocal(), in_data=None):
        try:
            if in_data is None:
                in_data = {}
            db_obj = Orderno_Sessionid(**in_data)
            db.add(db_obj)
            db.commit()
            db.flush(db_obj)
        except Exception as e:
            logger.exception(e)

    def __repr__(self):
        return f"kafka_bootstrap_servers:{self.bootstrap_servers}"
