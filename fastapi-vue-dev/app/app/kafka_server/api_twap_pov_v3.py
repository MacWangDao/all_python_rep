import base64
import itertools
import json
import math
import traceback
from abc import ABC
from typing import Optional, Mapping
import asyncio

from aiokafka import AIOKafkaProducer
from pandas import Timestamp
from loguru import logger

Text = str
from functools import wraps, partial

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError as e:
    pass
loop = asyncio.get_event_loop()


def timer(func=None, interval=1):
    if func is None:
        return partial(timer, interval=interval)

    @wraps(func)
    async def decorated(*args, **kwargs):
        while True:
            await asyncio.sleep(interval)
            await func(*args, **kwargs)

    return loop.create_task(decorated())


class Base(ABC):
    pass


class Order(Base):
    """委托定义"""

    strategy_id: Text = None
    """策略ID"""

    account_id: Text = None
    """账号ID"""

    account_name: Text = None
    """账户登录名"""

    channel_id: Text = None
    cl_ord_id: Text = None
    """委托客户端ID"""

    order_id: Text = None
    """委托柜台ID"""

    ex_ord_id: Text = None
    """委托交易所ID"""

    algo_order_id: Text = None
    """算法母单ID"""

    symbol: Text = None
    """symbol"""
    securityid: Text = None
    """symbol"""

    side: int = None
    """买卖方向，取值参考enum OrderSide"""

    position_effect: int = None
    """开平标志，取值参考enum PositionEffect"""

    position_side: int = None
    """持仓方向，取值参考enum PositionSide"""

    order_type: int = None
    """委托类型，取值参考enum OrderType"""

    order_business: int = None
    """委托业务分类，取值参考enum OrderBusiness"""

    order_duration: int = None
    """委托时间属性，取值参考enum OrderDuration"""

    order_qualifier: int = None
    """委托成交属性，取值参考enum OrderQualifier"""

    order_src: int = None
    """委托来源，取值参考enum OrderSrc"""

    position_src: int = None
    """头寸来源(仅适用融资融券)，取值参考enum PositionSrc"""

    status: int = None
    """委托状态，取值参考enum OrderStatus"""

    ord_rej_reason: int = None
    """委托拒绝原因，取值参考enum OrderRejectReason"""

    ord_rej_reason_detail: Text = None
    """委托拒绝原因描述"""

    price: float = None
    """委托价格"""

    stop_price: float = None
    """委托止损/止盈触发价格"""

    order_style: int = None
    """委托风格，取值参考 enum OrderStyle"""

    volume: int = None
    """委托量"""

    value: float = None
    """委托额"""

    percent: float = None
    """委托百分比"""

    target_volume: int = None
    """委托目标量"""

    target_value: float = None
    """委托目标额"""

    target_percent: float = None
    """委托目标百分比"""

    filled_volume: int = None
    """已成量"""

    filled_vwap: float = None
    """已成均价"""

    filled_amount: float = None
    """已成金额"""

    filled_commission: float = None
    """已成手续费"""

    bsdir: int = 1

    exchangeid: int = 1

    @property
    def created_at(self) -> Timestamp:
        """委托创建时间"""
        pass

    @property
    def updated_at(self) -> Timestamp:
        """委托更新时间"""
        pass

    def __init__(self,
                 bsdir: int = 1,
                 exchangeid: int = 1,
                 securityid: Text = "",
                 strategy_id: Text = None,
                 account_id: Text = None,
                 account_name: Text = None,
                 channel_id: Text = None,
                 cl_ord_id: Text = None,
                 order_id: Text = None,
                 ex_ord_id: Text = None,
                 algo_order_id: Text = None,
                 symbol: Text = None,
                 side: int = None,
                 position_effect: int = None,
                 position_side: int = None,
                 order_type: int = None,
                 order_business: int = None,
                 order_duration: int = None,
                 order_qualifier: int = None,
                 order_src: int = None,
                 position_src: int = None,
                 status: int = None,
                 ord_rej_reason: int = None,
                 ord_rej_reason_detail: Text = None,
                 price: float = None,
                 stop_price: float = None,
                 order_style: int = None,
                 volume: int = None,
                 value: float = None,
                 percent: float = None,
                 target_volume: int = None,
                 target_value: float = None,
                 target_percent: float = None,
                 filled_volume: int = None,
                 filled_vwap: float = None,
                 filled_amount: float = None,
                 filled_commission: float = None,
                 properties: Optional[Mapping[Text, Text]] = None,
                 created_at: Optional[Timestamp] = None,
                 updated_at: Optional[Timestamp] = None,
                 ) -> None: pass


def split_integer(m, n):
    assert n > 0
    quotient = int(m / n)
    remainder = m % n
    if remainder > 0:
        return [quotient] * (n - remainder) + [quotient + 1] * remainder
    if remainder < 0:
        return [quotient - 1] * -remainder + [quotient] * (n + remainder)
    return [quotient] * n


class ProducerClient:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    async def producer_client_send(self, key=None, value=None):
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                    key_serializer=lambda k: json.dumps(k).encode(),
                                    value_serializer=lambda v: json.dumps(v).encode())
        await producer.start()
        try:
            if value is not None:
                asyncio.ensure_future(producer.send(self.topic, key=key, value=value))
        except Exception as e:
            logger.error(e)
        finally:
            await producer.stop()


class TWAP(Base):

    def __init__(self, order: Order = None, order_cycle: int = 0, step_size: int = 0, cancellation_time: int = 0,
                 limit_complete: bool = False,
                 precedence: bool = False):
        self.order = order
        self.order_cycle = order_cycle
        self.step_size = step_size
        self.cancellation_cycle_time = cancellation_time
        self.limit_complete = limit_complete
        self.precedence = precedence
        self.cycle_count = math.ceil(self.order_cycle / self.step_size) if self.step_size != 0 else 0
        self.volume_sum = 0
        # self.volume_size = 0
        self.order_id_counter = None
        self.producer = None
        self.producer_client = None
        self.send_topic = None
        self.consumer_query = None
        self.job_msg = {}
        self.order_info = {}
        self.login_info = None
        self.job_id = None
        self.tvolume = 0
        self.result = {}
        self.orderno_list = []
        self.complete_count = 0
        self.limit_count = 1
        self.redis = None
        self.ssid = None
        self.userid = None
        self.pause_ssid = None
        # self.tvolume = self.trade_volume()
        self.check_volume()

    def check_order(self):
        self.treade_volume_list = split_integer(self.order.volume, self.cycle_count)

    def check_volume(self):
        self.volume_size = round(self.order.volume / (self.order_cycle / self.step_size))
        if self.order.volume == 100:
            self.volume_size = 100
        if self.order.volume < 100:
            self.volume_size = 0
        if self.order.volume > 100:
            now_trade_volume = int(self.volume_size // 100 * 100)
            if now_trade_volume == 0:
                self.volume_size = self.order.volume
            elif now_trade_volume > 0:
                self.volume_size = now_trade_volume

    def volume_size(self):
        if self.volume_size % 100 == 0:
            return True
        else:
            return False

    @logger.catch
    def check_order_reduce(self):
        if self.volume_size > 0:
            if self.order.volume > self.volume_sum:
                volume_margin = math.fabs(self.order.volume - self.volume_sum - self.volume_size)
                self.volume_sum += self.volume_size
                return volume_margin
            else:
                return -1
        else:
            logger.error("volume:不是100的整数倍")
            return -1

    def force_completion(self, order_id, precedence):
        pass

    async def force_completion_async(self, order_id, precedence):
        pass

    @logger.catch
    async def track_order_async_v3(self, order_id):
        await asyncio.sleep(1)
        asyncio.ensure_future(self.query_order_async_v3(order_id))

    @logger.catch
    async def query_order_async_v3(self, order_id):
        await asyncio.sleep(self.cancellation_cycle_time / 1000)
        if self.order_info.get(order_id, {}).get("status") == 1 and self.order_info.get(order_id, {}).get("type") == 6:
            query_order = self.send_msg(type=13, securityid=self.order.securityid, price=None, volume=None,
                                        orderid=order_id)
            query_order["orderno"] = self.order_info[order_id]["orderno"]
            query_order["sessionid"] = self.order_info[order_id]["sessionid"]
            # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order))
            asyncio.ensure_future(self.cancel_order_async_v3(order_id))
            asyncio.ensure_future(self.query_order_result(order_id))
        else:
            await asyncio.sleep(5)
            asyncio.ensure_future(self.cancel_order_async_v3(order_id))
            asyncio.ensure_future(self.query_order_result(order_id))

    @logger.catch
    async def cancel_order_async_v3(self, order_id):
        try:
            logger.info(f"cancel_order:{order_id}")
            cancel_order = self.send_msg(type=7, securityid=self.order.securityid, price=None, volume=None,
                                         orderid=order_id)
            if self.order_info.get(order_id, {}).get("orderno"):
                cancel_order["orderno"] = self.order_info[order_id]["orderno"]
                cancel_order["sessionid"] = self.order_info[order_id]["sessionid"]
                # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))
                asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                asyncio.ensure_future(self.query_cancel_order_async(order_id))
            else:
                await asyncio.sleep(3)
                logger.info(f"cancel_order:{order_id}")
                if self.order_info.get(order_id, {}).get("orderno"):
                    cancel_order["orderno"] = self.order_info[order_id]["orderno"]
                    cancel_order["sessionid"] = self.order_info[order_id]["sessionid"]
                    # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))
                    asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                    asyncio.ensure_future(self.query_cancel_order_async(order_id))
                else:
                    logger.warning(f"cancel_order_async_v3:order_id:{order_id},no result!!!")
        except:
            logger.error(f"cancel_order:order_id:{order_id}")

    @logger.catch
    async def cancel_order_async_limit(self, order_id):
        try:
            logger.info(f"cancel_order_async_limit:{order_id}")
            cancel_order = self.send_msg(type=7, securityid=self.order.securityid, price=None, volume=None,
                                         orderid=order_id)
            if self.order_info.get(order_id, {}).get("orderno"):
                cancel_order["orderno"] = self.order_info[order_id]["orderno"]
                cancel_order["sessionid"] = self.order_info[order_id]["sessionid"]
                asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))
            else:
                await asyncio.sleep(3)
                logger.info(f"cancel_order:{order_id}")
                if self.order_info.get(order_id, {}).get("orderno"):
                    cancel_order["orderno"] = self.order_info[order_id]["orderno"]
                    cancel_order["sessionid"] = self.order_info[order_id]["sessionid"]
                    asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                    # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))
                else:
                    logger.warning(f"cancel_order_async_limit:order_id:{order_id},no result!!!")
        except:
            logger.info(f"cancel_order_async_limit:order_id:{order_id}")

    @logger.catch
    async def query_cancel_order_async(self, order_id):
        try:
            logger.info(f"query_cancel_order:{order_id}")
            await asyncio.sleep(0.12)
            query_order = self.send_msg(type=13, securityid=self.order.securityid, price=None, volume=None,
                                        orderid=order_id)
            if self.order_info.get(order_id, {}).get("orderno"):
                query_order["orderno"] = self.order_info[order_id]["orderno"]
                query_order["sessionid"] = self.order_info[order_id]["sessionid"]
                asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order))
                # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order))
                await asyncio.sleep(0.888)
                if self.order_info.get(order_id, {}).get("status") == 1 and self.order_info.get(order_id, {}).get(
                        "type") == 14:
                    orderstatus = [0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13, 14]
                    if self.order_info.get(order_id, {}).get("orderstatus") in orderstatus:
                        cancel_order = self.send_msg(type=7, securityid=self.order.securityid, price=None, volume=None,
                                                     orderid=order_id)
                        cancel_order["orderno"] = self.order_info[order_id]["orderno"]
                        cancel_order["sessionid"] = self.order_info[order_id]["sessionid"]
                        asyncio.ensure_future(
                            self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                        # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))
            else:
                await asyncio.sleep(3)
                logger.info(f"query_cancel_order:{order_id}")
                if self.order_info.get(order_id, {}).get("orderno"):
                    query_order["orderno"] = self.order_info[order_id]["orderno"]
                    query_order["sessionid"] = self.order_info[order_id]["sessionid"]
                    asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order))
                    # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order))
                    await asyncio.sleep(0.564)
                    if self.order_info.get(order_id, {}).get("status") == 1 and self.order_info.get(order_id, {}).get(
                            "type") == 14:
                        orderstatus = [0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13, 14]
                        if self.order_info.get(order_id, {}).get("orderstatus") in orderstatus:
                            cancel_order = self.send_msg(type=7, securityid=self.order.securityid, price=None,
                                                         volume=None,
                                                         orderid=order_id)
                            cancel_order["orderno"] = self.order_info[order_id]["orderno"]
                            cancel_order["sessionid"] = self.order_info[order_id]["sessionid"]
                            asyncio.ensure_future(
                                self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                            # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))
                else:
                    logger.warning(f"query_cancel_order:order_id:{order_id},no result!!!")
        except:
            logger.error(f"query_cancel_order:order_id:{order_id}")

    @logger.catch
    async def query_order_result(self, order_id):
        await asyncio.sleep(0.87)
        try:
            query_order_res = self.send_msg(type=13, securityid=self.order.securityid, price=None, volume=None,
                                            orderid=order_id)
            if self.order_info.get(order_id, {}).get("orderno"):
                query_order_res["orderno"] = self.order_info[order_id]["orderno"]
                query_order_res["sessionid"] = self.order_info[order_id]["sessionid"]
                asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order_res))
                # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order_res))
                asyncio.ensure_future(self.get_order_result())
            else:
                await asyncio.sleep(3)
                if self.order_info.get(order_id, {}).get("orderno"):
                    query_order_res["orderno"] = self.order_info[order_id]["orderno"]
                    query_order_res["sessionid"] = self.order_info[order_id]["sessionid"]
                    asyncio.ensure_future(
                        self.producer_client.producer_client_send(key=order_id, value=query_order_res))
                    # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order_res))
                    asyncio.ensure_future(self.get_order_result())
                else:
                    logger.warning(f"query_order_result:order_id:{order_id},no result!!!")

        except:
            logger.error(f"query_order_result:order_id:{order_id}")

    @logger.catch
    async def get_order_result(self):
        await asyncio.sleep(1)
        res_vol = self.job_msg.get(self.job_id)
        for orderstatus in res_vol:
            if orderstatus.get("orderstatus") in [7, 9]:
                if self.order_info.get(orderstatus.get("order_id")):
                    self.order_info.pop(orderstatus.get("order_id"))
        ordervol = [res.get("ordervol", 0) for res in res_vol]
        tradevol = [res.get("tradevol", 0) for res in res_vol]
        logger.info(f"completed:{sum(tradevol)}")

    def send_msg(self, type=None, securityid="", price=None, volume=None, orderid=None):
        order = {"type": type, "reqid": 2,
                 "accountInfo": self.login_info.get("accountInfo"), "securityid": securityid,
                 "exchangeid": self.order.exchangeid,
                 "bsdir": self.order.bsdir,
                 "price": price,
                 "volume": volume, "stockaccount": "", "orderid": orderid, "actionid": 1, "securitytype": 1}
        return order

    @logger.catch
    async def complete_order(self, order_id, volume):
        logger.info(f"send_order-order_id:{order_id}")
        self.order_info[order_id] = {"order_id": order_id, "status": 1, "volume": volume, "ssid": self.ssid,
                                     "userid": self.userid, "job_id": self.job_id}
        if not self.job_msg.get(self.job_id):
            self.job_msg[self.job_id] = [self.order_info[order_id]]
        else:
            self.job_msg[self.job_id].append(self.order_info[order_id])
        lastprice = await self.get_market_price()
        if lastprice is None:
            return
        order = self.send_msg(type=5, securityid=self.order.securityid, price=lastprice, volume=volume,
                              orderid=order_id)
        # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=order))
        asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=order))
        logger.info(order)

    @logger.catch
    async def query_complete_order(self, order_id):
        await asyncio.sleep(self.cancellation_cycle_time / 1000)
        asyncio.ensure_future(self.cancel_order_async_limit(order_id))
        asyncio.ensure_future(self.query_cancel_order_async(order_id))
        try:
            query_order = self.send_msg(type=13, securityid=self.order.securityid, price=None, volume=None,
                                        orderid=order_id)
            if self.order_info.get(order_id, {}).get("orderno"):
                query_order["orderno"] = self.order_info[order_id]["orderno"]
                query_order["sessionid"] = self.order_info[order_id]["sessionid"]
                asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order))
                # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order))
            else:
                await asyncio.sleep(3)
                if self.order_info.get(order_id, {}).get("orderno"):
                    asyncio.ensure_future(self.cancel_order_async_limit(order_id))
                    asyncio.ensure_future(self.query_cancel_order_async(order_id))
                    query_order["orderno"] = self.order_info[order_id]["orderno"]
                    query_order["sessionid"] = self.order_info[order_id]["sessionid"]
                    asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order))
                    # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order))
                else:
                    logger.warning(f"query_complete_order:order_id:{order_id},no result!!!!")
        except Exception as e:
            logger.error(e)
            logger.error(f"query_complete_order:order_id:{order_id}")

    @logger.catch
    async def limit_complete_order(self, order_id, volume):

        asyncio.ensure_future(self.send_partition_order(order_id, volume))
        await asyncio.sleep(0.1)
        asyncio.ensure_future(self.query_complete_order_res(order_id))

    async def send_partition_order(self, order_id, volume):
        logger.info(f"limit_send_order-order_id:{order_id}")
        lastprice = await self.get_market_price()
        if lastprice is not None:
            order = self.send_msg(type=5, securityid=self.order.securityid, price=lastprice,
                                  volume=volume,
                                  orderid=order_id)
            logger.info(order)
            # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=order))
            asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=order))
            self.order_info[order_id]["complete_count"] += 1
            self.order_info[order_id]["ssid"] = self.ssid
            self.order_info[order_id]["userid"] = self.userid
            self.order_info[order_id]["job_id"] = self.job_id
            await asyncio.sleep(0.1)
            asyncio.ensure_future(self.query_complete_order(order_id))

    @logger.catch
    async def get_market_price(self):
        if self.order.price is not None:
            return self.order.price
        security = base64.b64encode(self.order.securityid.encode()).decode()
        lastprice = await self.redis.hget(security, "lastprice")
        if lastprice is not None:
            lastprice = float(lastprice)
        else:
            logger.error("redis price is None!!!")
        return lastprice

    @logger.catch
    async def query_complete_order_res(self, order_id):
        await asyncio.sleep(self.cancellation_cycle_time / 1000 + 3)
        logger.info(f"query_complete_order_res:order_id:{order_id}")
        try:
            if self.order_info.get(order_id, {}).get("status") == 1 and self.order_info.get(order_id, {}).get(
                    "type") == 14:
                orderno = self.order_info.get(order_id, {}).get("orderno")
                if orderno is not None:
                    if orderno not in self.orderno_list:
                        self.orderno_list.append(orderno)
                        tradevol = self.order_info.get(order_id, {}).get("tradevol", 0)
                        ordervol = self.order_info.get(order_id, {}).get("ordervol", 0)
                        if tradevol > 0:
                            if not self.result.get("tradevol"):
                                self.result["tradevol"] = [tradevol]
                            else:
                                self.result["tradevol"].append(tradevol)
                            logger.info(f"completed:{sum(self.result['tradevol'])}")
                        trade_volume = ordervol - tradevol
                        # logger.info(trade_volume)
                        if trade_volume > 0:
                            if self.order_info[order_id]["complete_count"] < self.limit_count:
                                await asyncio.sleep(1)
                                asyncio.ensure_future(self.send_partition_order(order_id, trade_volume))
                            else:
                                orderstatus = self.order_info.get(order_id, {}).get("orderstatus")
                                if orderstatus in [7, 9]:
                                    if self.order_info.get(order_id):
                                        self.order_info.pop(order_id)
                                return
                orderstatus = self.order_info.get(order_id, {}).get("orderstatus")
                if orderstatus in [9]:
                    if self.order_info.get(order_id):
                        self.order_info.pop(order_id)
                    return
            else:
                return
            if not self.order_info.get(order_id, {}).get("sessionid"):
                return
            asyncio.ensure_future(self.query_complete_order_res(order_id))
        except:
            return

    @logger.catch
    async def trade_order_async_v3(self, order_id, volume):
        if not self.limit_complete:
            asyncio.ensure_future(self.complete_order(order_id, volume))
            if self.cancellation_cycle_time == -1:
                return
            asyncio.ensure_future(self.track_order_async_v3(order_id))
        else:
            if self.cancellation_cycle_time == -1:
                return
            self.tvolume = volume
            self.order_info[order_id] = {"complete_count": self.complete_count}
            asyncio.ensure_future(self.limit_complete_order(order_id, volume))

    @staticmethod
    def completed_done_result(job):
        pass

    @logger.catch
    async def job_order_async(self):
        volume_margin = self.check_order_reduce()
        if volume_margin < 0:
            return
        # order_id = next(self.order_id_counter)
        await self.redis.incr("order_id", amount=1)
        order_id = await self.redis.mget("order_id")
        order_id = int(order_id[0])
        logger.info(f"on_put_order(volume:{self.volume_size},order_id:{order_id},send_sum:{self.volume_sum})")
        asyncio.ensure_future(self.trade_order_async_v3(order_id, self.volume_size))
        await asyncio.sleep(self.step_size / 1000)
        asyncio.ensure_future(self.job_order_async())

    async def consumer_request_async(self):

        logger.info(self.tvolume)
        if self.tvolume == 3:
            return
        await asyncio.sleep(3)
        self.tvolume += 1
        asyncio.ensure_future(self.consumer_request_async())

    async def consumer__response_async(self):
        pass

    def __del__(self):
        pass


class PoV(Base):

    def __init__(self, order: Order = None, transaction_rate: int = 0, step_size: int = 0, cancellation_time: int = 0,
                 bookkeep: bool = False):
        self.order = order
        self.transaction_rate = transaction_rate
        self.step_size = step_size
        self.cancellation_time = cancellation_time
        self.bookkeep = bookkeep
        self.order_id_counter = itertools.count()
        self.producer = None
        self.send_topic = None
        self.consumer_query = None
        self.order_info = {}
        self.login_info = None
        self.job_id = None
        self.tvolume = self.trade_volume()
        self.result = {}
        self.complete_count = 0
        self.limit_count = 1
        self.limit_sub_count = 1
        self.orderno_list = []
        self.redis = None
        self.ssid = None
        self.userid = None
        self.pause_ssid = None
        self.producer_client = None

    async def get_oredr_all_market(self):
        security = base64.b64encode(self.order.securityid.encode()).decode()
        volume = await self.redis.hget(security, "volume")
        if volume is not None:
            volume = int(volume)
        return volume

    def trade_volume(self):
        return self.order.volume

    async def included_self_volume(self):
        market_order_volume = await self.get_oredr_all_market()
        if market_order_volume is not None:
            if self.bookkeep:
                market_order_volume += self.tvolume
            now_trade_volume = market_order_volume * (self.transaction_rate / 100)
            now_trade_volume = int(now_trade_volume // 100 * 100)
            return now_trade_volume

    def send_msg(self, type=None, securityid="", price=None, volume=None, orderid=None):
        order = {"type": type, "reqid": 2,
                 "accountInfo": self.login_info.get("accountInfo"), "securityid": securityid,
                 "exchangeid": self.order.exchangeid,
                 "bsdir": self.order.bsdir,
                 "price": price,
                 "volume": volume, "stockaccount": "", "orderid": orderid, "actionid": 1, "securitytype": 1}
        return order

    @logger.catch
    async def trade_order_async(self, order_id, volume):
        logger.info(f"send_order-order_id:{order_id}")
        self.order_info[order_id] = {"order_id": order_id}
        lastprice = await self.get_market_price()
        if lastprice is None:
            return
        self.order_info[order_id]["complete_count"] = 1
        order = self.send_msg(type=5, securityid=self.order.securityid, price=lastprice, volume=volume,
                              orderid=order_id)

        logger.info(order)
        self.order_info[order_id]["ssid"] = self.ssid
        self.order_info[order_id]["userid"] = self.userid
        self.order_info[order_id]["job_id"] = self.job_id
        self.tvolume -= volume
        asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=order))
        # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=order))
        asyncio.ensure_future(self.query_order_async(order_id))
        asyncio.ensure_future(self.query_complete_order_res(order_id))

    @logger.catch
    async def query_order_async(self, order_id):
        await asyncio.sleep(1)
        try:
            query_order = self.send_msg(type=13, securityid=self.order.securityid, price=None, volume=None,
                                        orderid=order_id)
            if self.order_info.get(order_id, {}).get("orderno"):
                query_order["orderno"] = self.order_info[order_id]["orderno"]
                query_order["sessionid"] = self.order_info[order_id]["sessionid"]
                asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order))
                # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order))
                asyncio.ensure_future(self.cancel_order_async(order_id))
            else:
                await asyncio.sleep(4)
                if self.order_info.get(order_id, {}).get("orderno"):
                    query_order["orderno"] = self.order_info[order_id]["orderno"]
                    query_order["sessionid"] = self.order_info[order_id]["sessionid"]
                    asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order))
                    # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order))
                    asyncio.ensure_future(self.cancel_order_async(order_id))
                else:
                    logger.warning(f"query_order_async:order_id:{order_id},no result!!!")
        except:
            logger.error(f"query_order_async:order_id:{order_id},no result!!!")

    @logger.catch
    async def query_order_result(self, order_id):
        await asyncio.sleep(0.1)
        try:
            query_order_res = self.send_msg(type=13, securityid=self.order.securityid, price=None, volume=None,
                                            orderid=order_id)
            if self.order_info.get(order_id, {}).get("orderno"):
                query_order_res["orderno"] = self.order_info[order_id]["orderno"]
                query_order_res["sessionid"] = self.order_info[order_id]["sessionid"]
                asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order_res))
                # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order_res))
                asyncio.ensure_future(self.get_order_result(order_id))
            else:
                await asyncio.sleep(3)
                if self.order_info.get(order_id, {}).get("orderno"):
                    query_order_res["orderno"] = self.order_info[order_id]["orderno"]
                    query_order_res["sessionid"] = self.order_info[order_id]["sessionid"]
                    asyncio.ensure_future(
                        self.producer_client.producer_client_send(key=order_id, value=query_order_res))
                    # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order_res))
                    asyncio.ensure_future(self.get_order_result(order_id))
                else:
                    logger.warning(f"query_order_result:order_id:{order_id},no result!!!")
        except:
            logger.warning(f"query_order_result:order_id:{order_id}")

    @logger.catch
    async def get_order_result(self, order_id):
        await asyncio.sleep(0.5)
        if self.order_info.get(order_id):
            order = self.order_info.get(order_id)
            if not self.result.get("ordervol"):
                self.result["ordervol"] = [order.get('ordervol')]
            else:
                self.result["ordervol"].append(order.get('ordervol'))
            if not self.result.get("tradevol"):
                self.result["tradevol"] = [order.get('tradevol')]
            else:
                self.result["tradevol"].append(order.get('tradevol'))
            logger.info(f"ordervol:{order.get('ordervol')},tradevol:{order.get('tradevol')}")
            logger.info(f"completed:{sum(self.result.get('tradevol'))}")

    @logger.catch
    async def cancel_order_async(self, order_id):
        await asyncio.sleep(self.cancellation_time / 1000)
        try:
            logger.info(f"cancel_order-order_id:{order_id}")
            cancel_order = self.send_msg(type=7, securityid=self.order.securityid, price=None, volume=None,
                                         orderid=order_id)
            if self.order_info.get(order_id, {}).get("orderno"):
                cancel_order["orderno"] = self.order_info[order_id]["orderno"]
                cancel_order["sessionid"] = self.order_info[order_id]["sessionid"]
                if self.order_info.get(order_id, {}).get("status") == 1 and self.order_info.get(order_id, {}).get(
                        "type") == 14:
                    orderstatus = [0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13, 14]
                    if self.order_info.get(order_id, {}).get("orderstatus") in orderstatus:
                        asyncio.ensure_future(
                            self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                        # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))

                else:
                    await asyncio.sleep(self.cancellation_time / 1000 + 2)
                    asyncio.ensure_future(
                        self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                    # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))
            else:
                await asyncio.sleep(3)
                if self.order_info.get(order_id, {}).get("orderno"):
                    cancel_order["orderno"] = self.order_info[order_id]["orderno"]
                    cancel_order["sessionid"] = self.order_info[order_id]["sessionid"]
                    if self.order_info.get(order_id, {}).get("status") == 1 and self.order_info.get(order_id, {}).get(
                            "type") == 14:
                        orderstatus = [0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13, 14]
                        if self.order_info.get(order_id, {}).get("orderstatus") in orderstatus:
                            asyncio.ensure_future(
                                self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                            # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))

                    else:
                        await asyncio.sleep(self.cancellation_time / 1000 + 2)
                        asyncio.ensure_future(
                            self.producer_client.producer_client_send(key=order_id, value=cancel_order))
                        # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=cancel_order))
                else:
                    logger.warning(f"cancel_order_async-order_id:{order_id},no result!!!")
            asyncio.ensure_future(self.query_cancel_order_async(order_id))
            asyncio.ensure_future(self.query_order_result(order_id))
        except:
            logger.error(f"cancel_order_async-order_id:{order_id},no result!!!")

    @logger.catch
    async def query_cancel_order_async(self, order_id):
        try:
            logger.info(f"query_cancel_order-order_id:{order_id}")
            query_order = self.send_msg(type=13, securityid=self.order.securityid, price=None, volume=None,
                                        orderid=order_id)
            if self.order_info.get(order_id, {}).get("orderno"):
                query_order["orderno"] = self.order_info[order_id]["orderno"]
                query_order["sessionid"] = self.order_info[order_id]["sessionid"]
                asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order))
                # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order))
            else:
                await asyncio.sleep(3)
                if self.order_info.get(order_id, {}).get("orderno"):
                    query_order["orderno"] = self.order_info[order_id]["orderno"]
                    query_order["sessionid"] = self.order_info[order_id]["sessionid"]
                    asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=query_order))
                    # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=query_order))
                else:
                    logger.warning(f"query_cancel_order_async-order_id:{order_id},no result!!!")
        except:
            logger.error(f"query_cancel_order_async-order_id:{order_id},no result!!!")

    async def send_partition_no_finish_order(self, order_id, volume):
        await asyncio.sleep(self.step_size / 1000)
        logger.info(f"limit_send_order-order_id:{order_id}")
        lastprice = await self.get_market_price()
        if lastprice is not None:
            order = self.send_msg(type=5, securityid=self.order.securityid, price=lastprice,
                                  volume=volume,
                                  orderid=order_id)

            asyncio.ensure_future(self.producer_client.producer_client_send(key=order_id, value=order))
            logger.info(order)
            # asyncio.ensure_future(self.producer.send(self.send_topic, key=order_id, value=order))
            self.order_info[order_id]["complete_count"] += 1
            self.order_info[order_id]["ssid"] = self.ssid
            self.order_info[order_id]["userid"] = self.userid
            self.order_info[order_id]["job_id"] = self.job_id
            await asyncio.sleep(0.001)
            asyncio.ensure_future(self.check_order_again_complete(order_id))

    @logger.catch
    async def get_market_price(self):
        if self.order.price is not None:
            return self.order.price
        security = base64.b64encode(self.order.securityid.encode()).decode()
        lastprice = await self.redis.hget(security, "lastprice")
        if lastprice is not None:
            lastprice = float(lastprice)
        else:
            logger.error("redis price is None!!!")
        return lastprice

    async def check_order_again_complete(self, order_id):

        asyncio.ensure_future(self.query_order_async(order_id))

    @logger.catch
    async def query_complete_order_res(self, order_id):
        await asyncio.sleep(self.cancellation_time / 1000 + 3)
        logger.info(f"query_complete_order_res:order_id:{order_id}")
        try:
            if self.order_info.get(order_id, {}).get("status") == 1 and self.order_info.get(order_id, {}).get(
                    "type") == 14:
                orderno = self.order_info.get(order_id, {}).get("orderno")
                if orderno not in self.orderno_list:
                    self.orderno_list.append(orderno)
                    tradevol = self.order_info.get(order_id, {}).get("tradevol", 0)
                    ordervol = self.order_info.get(order_id, {}).get("ordervol", 0)
                    if tradevol > 0:
                        if not self.result.get("tradevol"):
                            self.result["tradevol"] = [tradevol]
                        else:
                            self.result["tradevol"].append(tradevol)
                        logger.info(f"completed:{sum(self.result['tradevol'])}")
                    trade_volume = ordervol - tradevol
                    logger.info(trade_volume)
                    if trade_volume > 0:
                        if self.order_info[order_id]["complete_count"] < self.limit_sub_count:
                            asyncio.ensure_future(self.send_partition_no_finish_order(order_id, trade_volume))
                        else:
                            orderstatus = self.order_info.get(order_id, {}).get("orderstatus")
                            if orderstatus in [7, 9]:
                                if self.order_info.get(order_id):
                                    self.order_info.pop(order_id)
                            return
                orderstatus = self.order_info.get(order_id, {}).get("orderstatus")
                if orderstatus in [9]:
                    if self.order_info.get(order_id):
                        self.order_info.pop(order_id)
                    return
            else:
                return

            if not self.order_info.get(order_id, {}).get("sessionid"):
                return
            asyncio.ensure_future(self.query_complete_order_res(order_id))
        except:
            return

    @logger.catch
    async def job_order_async(self):
        # print(self.tvolume)
        if self.tvolume > 0:
            if self.complete_count < self.limit_count:
                now_trade_volume = await self.included_self_volume()
                if not now_trade_volume:
                    logger.error("redis not volume!!!")
                    return
                if now_trade_volume >= self.tvolume:
                    now_trade_volume = self.tvolume
                self.complete_count += 1
            else:
                return
        else:
            return

        # order_id = next(self.order_id_counter)
        await self.redis.incr("order_id", amount=1)
        order_id = await self.redis.mget("order_id")
        order_id = int(order_id[0])
        logger.info(
            f"on_put_order(volume:{now_trade_volume},order_id:{order_id},send:{now_trade_volume})")
        asyncio.ensure_future(self.trade_order_async(order_id, now_trade_volume))
        await asyncio.sleep(self.step_size / 1000)
        asyncio.ensure_future(self.job_order_async())


class Job:
    def __init__(self):
        pass
