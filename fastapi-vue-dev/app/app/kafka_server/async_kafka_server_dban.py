import datetime
import itertools
import traceback

import aioredis
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
import json
from loguru import logger
from sqlalchemy.orm.session import Session
from app.models.strategy_stock import Orderno_Sessionid
from app.db.session import SessionLocal

from app.kafka_server.api_twap_pov_v4 import TWAP, PoV, Order


class ProducerClient:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    async def producer_client_send(self, key=None, value=None):
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, retry_backoff_ms=1000,
                                    key_serializer=lambda k: json.dumps(k).encode(),
                                    value_serializer=lambda v: json.dumps(v).encode())
        await producer.start()
        try:
            if value is not None:
                asyncio.ensure_future(producer.send(self.topic, key=key, value=value))
        except Exception as e:
            logger.exception(e)
        finally:
            await producer.stop()


class AsyncKafkaServerMN(object):
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

    def __init__(self, config):
        self.config = config
        self.redis_info = self.config.get("redis_info", {})
        self.redis_8 = aioredis.from_url(f"redis://{self.redis_info.get('host')}", db=8, encoding="utf-8",
                                         decode_responses=True)
        self.redis_9 = aioredis.from_url(f"redis://{self.redis_info.get('host')}", db=9, encoding="utf-8",
                                         decode_responses=True)
        self.redis_10 = aioredis.from_url(f"redis://{self.redis_info.get('host')}", db=10, encoding="utf-8",
                                          decode_responses=True)
        self.redis_0 = aioredis.from_url(f"redis://{self.redis_info.get('host')}", db=0, encoding="utf-8",
                                         decode_responses=True)
        self.bootstrap_servers = self.config.get("kafka_server", {}).get("bootstrap_servers", [])
        self.req_pipe = self.config.get("kafka_server", {}).get("topics", {}).get("req_pipe")
        self.req_pipe = f"simulation_{self.req_pipe}"
        self.rsp_pipe = self.config.get("kafka_server", {}).get("topics", {}).get("rsp_pipe")
        self.rsp_pipe = f"simulation_{self.rsp_pipe}"
        self.command_topic = self.config.get("kafka_server", {}).get("topics", {}).get("command_topic")
        self.command_topic = f"simulation_{self.command_topic}"

        self.twap_limit_count = self.config.get("twap_limit_count", 1)
        self.pov_limit_count = self.config.get("pov_limit_count", 1)
        self.pov_limit_sub_count = self.config.get("pov_limit_sub_count", 1)

    @logger.catch
    async def service(self):

        consumer_req = AIOKafkaConsumer(
            self.req_pipe,
            bootstrap_servers=self.bootstrap_servers,
            # rebalance_timeout_ms=1000,
            # session_timeout_ms=10000,
            group_id="req_pipe-02")
        await consumer_req.start()
        # await asyncio.sleep(0.1)
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, retry_backoff_ms=1000,
                                    key_serializer=lambda k: json.dumps(k).encode(),
                                    value_serializer=lambda v: json.dumps(v).encode())
        await producer.start()
        consumer_rsp = AIOKafkaConsumer(
            self.rsp_pipe,
            bootstrap_servers=self.bootstrap_servers,
            # rebalance_timeout_ms=1000,
            # session_timeout_ms=10000,
            enable_auto_commit=True, retry_backoff_ms=1000,
            group_id="rsp_pipe-02")
        await consumer_rsp.start()
        consumer_cmd = AIOKafkaConsumer(
            self.command_topic,
            # rebalance_timeout_ms=1000,
            # session_timeout_ms=10000,
            bootstrap_servers=self.bootstrap_servers, retry_backoff_ms=1000,
            group_id="command_td-02")
        await consumer_cmd.start()

        async def trade_request_async():
            async for request in consumer_cmd:
                try:
                    logger.info(
                        f"request consumed:{request.topic},{request.partition},{request.offset},{request.key},{request.value},{request.timestamp}")
                    job = self.create_job_kafka(request.value)
                    job.producer_client = ProducerClient(self.bootstrap_servers, self.req_pipe)
                    await self.redis_0.incr("job_id", amount=1)
                    job_id = await self.redis_0.mget("job_id")
                    job.job_id = int(job_id[0])
                    # job.order_id_counter = self.order_id_counter
                    # job.job_id = next(self.job_id_counter)
                    # await asyncio.sleep(1)
                    asyncio.ensure_future(job.job_order_async())
                except Exception as e:
                    logger.exception(e)

        @logger.catch
        async def consumer_request_async():
            async for request in consumer_req:
                try:
                    # logger.info(
                    #     f"request consumed:{request.topic},{request.partition},{request.offset},{request.key},{request.value},{request.timestamp}")
                    request = request.value.decode("utf-8")
                    request = json.loads(request)
                    logger.info(request)
                    ptype = request.get("type")
                    reqid = request.get("reqid")
                    accountInfo = request.get("accountInfo")
                    account = accountInfo.get("account")
                    if ptype == 3:  # 认证登录
                        logger.info("模拟账号登录")
                        login_data = {"type": 4, "reqid": reqid, "account": account, "securitytype": 2, "status": 1,
                                      "errorcode": 1,
                                      "error": ""}
                        asyncio.ensure_future(producer.send(self.rsp_pipe, key="simulation", value=login_data))
                    elif ptype == 5:  # 报单录入
                        hour = datetime.datetime.now().hour
                        minute = datetime.datetime.now().minute
                        second = datetime.datetime.now().second
                        tradetime = (hour * 3600 + minute * 60 + second) * 1000
                        securityid = request.get("securityid")
                        exchangeid = request.get("exchangeid")
                        bsdir = request.get("bsdir")
                        price = request.get("price")
                        volume = request.get("volume")
                        orderid = request.get("orderid")
                        await self.redis_8.incr("orderno", amount=1)
                        orderno = await self.redis_8.mget("orderno")
                        orderno = str(orderno[0])
                        mapping = {"securityid": securityid, "exchangeid": exchangeid, "bsdir": bsdir,
                                   "orderprice": price,
                                   "ordervolume": volume, "tradevolume": 0, "tradeprice": 0,
                                   "orderid": orderid, "orderno": orderno, "orderstatus": 1, "tradetime": tradetime}
                        if volume >= 1000:
                            mapping["orderstatus"] = 9
                            mapping["tradevolume"] = volume
                            mapping["tradeprice"] = price
                        tradevolume = mapping.get("tradevolume")
                        tradeprice = mapping.get("tradeprice")
                        tradevalue = tradevolume * tradeprice
                        accountbusiness = await self.redis_8.hgetall(account)
                        if bsdir == 1:
                            marketvalue = float(accountbusiness.get("marketvalue", 0))
                            marketvalue += tradevalue
                            available = float(accountbusiness.get("available", 0))
                            available -= tradevalue
                            totalasset = marketvalue + available
                            curbalance = tradevalue
                            await self.redis_8.hset(account, mapping={
                                "curbalance": curbalance,
                                "available": available,
                                "marketvalue": marketvalue,
                                "totalasset": totalasset
                            })
                        elif bsdir == 2:
                            marketvalue = float(accountbusiness.get("marketvalue", 0))
                            marketvalue -= tradevalue
                            available = float(accountbusiness.get("available", 0))
                            available += tradevalue
                            totalasset = marketvalue + available
                            curbalance = tradevalue
                            await self.redis_8.hset(account, mapping={
                                "curbalance": curbalance,
                                "available": available,
                                "marketvalue": marketvalue,
                                "totalasset": totalasset
                            })
                        await self.redis_8.hset(f"order-{account}-{orderno}", mapping=mapping)
                        await self.redis_8.expire(f"order-{account}-{orderno}", 43200)
                        # await self.redis.rpush("order", json.dumps(
                        #     {securityid: securityid, exchangeid: exchangeid, bsdir: bsdir, price: price, volume: volume,
                        #      orderid: orderid, orderno: orderno}))
                        if mapping["orderstatus"] == 9:
                            if bsdir == 1:
                                holdexist = await self.redis_8.exists(f"hold-{account}-{securityid}")
                                if not holdexist:
                                    hold = {
                                        "account": account,
                                        "availablevol": volume,
                                        "avbuyprice": price,
                                        "curvol": volume,
                                        "exchangeid": exchangeid,
                                        "securityid": securityid,
                                        "countprice": 1,
                                        "sumprice": price
                                    }
                                    await self.redis_8.hset(f"hold-{account}-{securityid}", mapping=hold)
                                else:

                                    trade = await self.redis_8.hmget(f"hold-{account}-{securityid}", "countprice",
                                                                     "sumprice", "availablevol", "curvol")
                                    countprice = int(trade[0]) if trade[0] is not None else 1
                                    countprice += 1
                                    sumprice = float(trade[1]) if trade[1] is not None else 0
                                    sumprice += price
                                    avbuyprice = round(sumprice / countprice, 2)

                                    availablevol = float(trade[2]) if trade[2] is not None else 1
                                    availablevol += volume
                                    curvol = float(trade[3]) if trade[3] is not None else 0
                                    curvol += volume
                                    await self.redis_8.hset(f"hold-{account}-{securityid}", key="countprice",
                                                            value=countprice)
                                    await self.redis_8.hset(f"hold-{account}-{securityid}", key="sumprice",
                                                            value=sumprice)
                                    await self.redis_8.hset(f"hold-{account}-{securityid}", key="avbuyprice",
                                                            value=avbuyprice)
                                    await self.redis_8.hset(f"hold-{account}-{securityid}", key="availablevol",
                                                            value=availablevol)
                                    await self.redis_8.hset(f"hold-{account}-{securityid}", key="curvol",
                                                            value=curvol)
                            if bsdir == 2:
                                trade = await self.redis_8.hmget(f"hold-{account}-{securityid}", "availablevol",
                                                                 "curvol")
                                if not trade:
                                    order_out = {"type": 6, "reqid": 2, "account": account, "actionid": 1,
                                                 "orderid": orderid,
                                                 "msgid": -1,
                                                 "orderno": None, "status": 2, "sessionid": 1, "errorcode": 1}
                                    asyncio.ensure_future(
                                        producer.send(self.rsp_pipe, key="simulation", value=order_out))
                                    continue

                                availablevol = float(trade[0]) if trade[0] is not None else 0
                                availablevol -= volume
                                curvol = float(trade[1]) if trade[1] is not None else 0
                                curvol -= volume
                                await self.redis_8.hset(f"hold-{account}-{securityid}", key="availablevol",
                                                        value=availablevol)
                                await self.redis_8.hset(f"hold-{account}-{securityid}", key="curvol",
                                                        value=curvol)
                        order_out = {"type": 6, "reqid": 2, "account": account, "actionid": 1, "orderid": orderid,
                                     "msgid": -1,
                                     "orderno": orderno, "status": 1, "sessionid": 1, "errorcode": 1}
                        asyncio.ensure_future(producer.send(self.rsp_pipe, key="simulation", value=order_out))
                    elif ptype == 7:  # 报单撤单
                        orderid = request.get("orderid")
                        orderno = request.get("orderno")
                        name = f"order-{account}-{orderno}"
                        trade = await self.redis_8.hmget(name, "tradevolume", "tradeprice", "bsdir")
                        tradevolume = int(trade[0]) if trade[0] is not None else 0
                        if tradevolume < 1000:
                            tradeprice = float(trade[1]) if trade[1] is not None else 0
                            tradevalue = tradevolume * tradeprice
                            accountbusiness = await self.redis_8.hgetall(account)
                            bsdir = trade[2]
                            if bsdir == 1:
                                marketvalue = float(accountbusiness.get("marketvalue", 0))
                                marketvalue -= tradevalue
                                available = float(accountbusiness.get("available", 0))
                                available += tradevalue
                                totalasset = marketvalue + available
                                curbalance = tradevalue
                                await self.redis_8.hset(account, mapping={
                                    "curbalance": curbalance,
                                    "available": available,
                                    "marketvalue": marketvalue,
                                    "totalasset": totalasset
                                })
                            elif bsdir == 2:
                                marketvalue = float(accountbusiness.get("marketvalue", 0))
                                marketvalue += tradevalue
                                available = float(accountbusiness.get("available", 0))
                                available -= tradevalue
                                totalasset = marketvalue + available
                                curbalance = tradevalue
                                await self.redis_8.hset(account, mapping={
                                    "curbalance": curbalance,
                                    "available": available,
                                    "marketvalue": marketvalue,
                                    "totalasset": totalasset
                                })
                            await self.redis_8.hset(name, key="orderstatus", value=7)
                            cancel = {"type": 8, "reqid": reqid, "actionid": 1, "orderid": orderid, "msgid": -1,
                                      "orderno": orderno,
                                      "status": 1, "errorcode": 1, "error": ""}
                            asyncio.ensure_future(producer.send(self.rsp_pipe, key="simulation", value=cancel))
                        else:
                            cancel = {"type": 8, "reqid": reqid, "actionid": 1, "orderid": orderid, "msgid": -1,
                                      "orderno": orderno,
                                      "status": 2, "errorcode": 1, "error": "委托状态错误不能撤单"}
                            asyncio.ensure_future(producer.send(self.rsp_pipe, key="simulation", value=cancel))
                    elif ptype == 9:  # 持仓查询
                        holdInfo = []
                        keys = await self.redis_8.keys(f"hold-{account}-*")
                        for k in keys:
                            val = await self.redis_8.hgetall(k)
                            holdInfo.append(
                                {
                                    "account": val.get("account"),
                                    "availablevol": val.get("availablevol"),
                                    "avbuyprice": val.get("avbuyprice"),
                                    "curvol": val.get("curvol"),
                                    "exchangeid": val.get("exchangeid"),
                                    "securityid": val.get("securityid")
                                }
                            )
                        query = {
                            "type": 10,
                            "reqid": 1,
                            "msgid": -1,
                            "account": account,
                            "holdInfo": holdInfo,
                            "isfinal": True,
                            "status": 1
                        }

                        asyncio.ensure_future(producer.send(self.rsp_pipe, key="simulation", value=query))
                    elif ptype == 11:  # 资金查询
                        accountbusiness = await self.redis_8.hgetall(account)
                        query = {
                            "type": 12,
                            "reqid": 1,
                            "msgid": -1,
                            "account": account,
                            "isfinal": True,
                            "status": 1,
                            "errorcode": 1,
                            "error": "",
                            "accountbusiness": accountbusiness
                        }

                        asyncio.ensure_future(producer.send(self.rsp_pipe, key="simulation", value=query))
                    elif ptype == 13:  # 报单查询
                        orderInfo = []
                        orderno = request.get("orderno")
                        if orderno:
                            keys = await self.redis_8.keys(f"order-{account}-{orderno}")
                        else:
                            keys = await self.redis_8.keys(f"order-{account}-*")
                        for k in keys:
                            val = await self.redis_8.hgetall(k)
                            orderInfo.append(
                                {
                                    "securityid": val.get("securityid"),
                                    "orderno": val.get("orderno"),
                                    "bsdir": val.get("bsdir"),
                                    "orderprice": val.get("orderprice"),
                                    "ordervol": int(val.get("ordervolume")),
                                    "tradevol": int(val.get("tradevolume")),
                                    "tradeprice": val.get("tradeprice"),
                                    "orderstatus": int(val.get("orderstatus", 0)),
                                    "pricdemode": 1,
                                    "exchangeid": val.get("exchangeid"),
                                    "account": account,
                                    "sessionid": 1
                                }
                            )
                        query = {
                            "type": 14,
                            "reqid": 1,
                            "account": account,
                            "orderid": request.get("orderid"),
                            "msgid": -1,
                            "status": 1,
                            "errorcode": 1,
                            "error": "",
                            "orderInfo": orderInfo
                        }

                        asyncio.ensure_future(producer.send(self.rsp_pipe, key="simulation", value=query))
                    elif ptype == 15:  # 成交查询
                        keys = await self.redis_8.keys(f"order-{account}-*")
                        tradeInfo = []
                        for k in keys:
                            val = await self.redis_8.hgetall(k)
                            if int(val.get("orderstatus", 0)) == 9:
                                tradeInfo.append(
                                    {
                                        "securityid": val.get("securityid"),
                                        "orderno": val.get("orderno"),
                                        "bsdir": val.get("bsdir"),
                                        "price": val.get("tradeprice"),
                                        "volume": int(val.get("tradevolume", 0)),
                                        "exchangeid": val.get("exchangeid"),
                                        "account": account,
                                        "tradeid": "1",
                                        "tradetime": val.get("tradetime"),
                                        "orderid": val.get("orderid"),
                                        "tradetype": 1,
                                        "isquerybak": False
                                    }
                                )
                        query = {
                            "type": 16,
                            "reqid": 1,
                            "actionid": 1,
                            "msgid": -1,
                            "account": account,
                            "status": 1,
                            "errorcode": 1,
                            "error": "",
                            "tradeInfo": tradeInfo
                        }

                        asyncio.ensure_future(producer.send(self.rsp_pipe, key="simulation", value=query))
                    else:
                        pass
                except Exception as e:
                    logger.exception(e)

        async def consumer_response_async():
            async for response in consumer_rsp:
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
                            logger.info("模拟账户登录:success")
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
                                in_data = {"userid": self.global_order_info.get(order_id, {}).get("userid"),
                                           "ssid": self.global_order_info.get(order_id, {}).get("ssid"),
                                           "batid": self.global_order_info.get(order_id, {}).get("batid"),
                                           "job_id": self.global_order_info.get(order_id, {}).get("job_id"),
                                           "order_id": order_id, "orderno": order_response.get("orderno"),
                                           "sessionid": order_response.get("sessionid"), "status": status,
                                           "error_msg": order_response.get("error", "")}
                                orderno = order_response.get("orderno")
                                ctime = datetime.datetime.now()
                                if orderno is not None:
                                    in_data.update({"ctime": ctime.strftime("%Y-%m-%d %H:%M:%S")})
                                    if self.global_order_info.get(order_id, {}).get("not_cancel"):
                                        in_data.update({"not_cancel": 1})
                                    await self.redis_9.hset(orderno, mapping=in_data)
                                in_data.update({"ctime": ctime})
                                in_data.pop("not_cancel", None)
                                self.to_database(in_data=in_data)
                    elif order_response.get("type") == 8:  # 撤单
                        self.cancel_order.clear()
                        self.cancel_order.append(order_response)
                    elif order_response.get("type") == 14:  # 报单查询
                        self.query_order.clear()
                        self.query_order = order_response
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
                                    asyncio.ensure_future(self.redis_10.publish("redis_oracle:1", json.dumps(order)))
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
            asyncio.ensure_future(trade_request_async())
            asyncio.ensure_future(consumer_request_async())
            await consumer_response_async()

        except Exception as e:
            logger.error(e)
        finally:
            await producer.stop()
            await consumer_req.stop()
            await consumer_rsp.stop()
            await consumer_cmd.stop()

    async def producer_client(self, key, value, ptype=1):
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            retry_backoff_ms=1000,
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

    def run(self):
        asyncio.ensure_future(self.service())

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
                job.redis = self.redis_0
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
                job.redis = self.redis_0
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
