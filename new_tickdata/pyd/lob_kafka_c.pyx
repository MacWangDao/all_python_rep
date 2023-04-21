# cython:language_level=3
#python setup.py build_ext --inplace
#cythonize -a -i lob_kafka_c.pyx
from __future__ import print_function
import os
from concurrent.futures import ProcessPoolExecutor

import yaml
from influxdb_client.client.write_api import ASYNCHRONOUS
from kafka import KafkaConsumer, TopicPartition

from dtutils import tdfdt_to_pddt, is_hour_between, get_datetime_str
from functools import reduce
from funcutils import pad_or_truncate, full_merge_dict
from loguru import logger
from influxdb_client import InfluxDBClient, Point, WritePrecision
import numpy as np
import pandas as pd
from oracleutils import get_lclose_price
from pykutils import local_time_timestamp, protobuf_order_parse, parse_pb, protobuf_transactions_parse

logger.remove(handler_id=None)
logger.add("lob5.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")
time_interval = 3


class Order:
    '''
    self.status can be new, partial, complete, cancel
    '''

    def __init__(self, bs_type, kind, place_datetime, norder, price, total_volume):
        self.bs_type = bs_type
        self.kind = kind
        self.place_datetime = place_datetime
        self.last_update_datetime = place_datetime
        self.create_datetime = place_datetime
        self.norder = norder
        self.price = price
        self.total_volume = total_volume
        self.left_volume = total_volume

        # self.trans_count = 0  #how many times this order is involved in transactions.
        self.trans_detail = []

        self.status = 'new'

    def cancel_order(self, cancel_datetime, volume):
        self.left_volume -= volume
        self.status = 'cancel'
        self.last_update_datetime = cancel_datetime
        return self.left_volume

    # @logger.catch
    def confirm_transaction(self, trans_datetime, volume, price):
        if volume > self.left_volume:
            return -1  # error! should not be larger than the left_volume
        if self.status != 'new' and self.status != 'partial':
            return -2  # error! only accept trans on NEW or PARTIALly filled orders.
        # self.trans_count += 1
        self.left_volume -= volume
        self.status = 'partial'
        if self.left_volume <= 0:  # should only equal to 0
            self.status = 'complete'
        self.trans_detail.append((price, volume, trans_datetime))
        self.last_update_datetime = trans_datetime
        return self.left_volume


cdef class LimitOrderBook:
    cdef public:
        int exchange
        str code
        str date
        list cached_orders
        list cached_trans
        dict dynamic_on_prices
        dict orderbook_on_prices
        dict all_orders
        float last_update_datetime
        float last_order_datetime
        float last_trans_datetime
        double last_price
        double open_price
        double high_price
        double low_price
        double close_price
        list ask_prices_queue_incr
        list bid_prices_queue_decr
    def __init__(self, int exchange, str code, str date):
        self.exchange = exchange
        self.code = code
        self.date = date

        self.cached_orders = []
        self.cached_trans = []

        self.dynamic_on_prices = {}
        self.orderbook_on_prices = {}  # grouped by price{ 10.00:{orderid:order}}

        self.all_orders = {}

        self.last_update_datetime = 0
        self.last_order_datetime = pd.to_datetime('1900-1-1 00:00 +0800').timestamp()  # for aligning time
        self.last_trans_datetime = pd.to_datetime('1900-1-1 00:00 +0800').timestamp()  # for aligning time
        self.last_price = 0.0

        # ohlc
        self.open_price = 0.0
        self.high_price = 0.0
        self.low_price = 0.0
        self.close_price = 0.0

        self.ask_prices_queue_incr = []
        self.bid_prices_queue_decr = []

    # @logger.catch
    def calc_left_volume_on_price(self, price, bs_type):
        orderdict = self.orderbook_on_prices[price]
        return reduce(lambda x, value: x + value.left_volume if value.bs_type == bs_type else 0, orderdict.values(), 0)

    # call when new order in new price is placing or removing a price(no orders left)
    # @logger.catch
    cdef update_prices_queue(self):
        self.ask_prices_queue_incr = []
        self.bid_prices_queue_decr = []
        all_prices = self.orderbook_on_prices.keys()
        for price in all_prices:
            if price <= self.last_price:
                bid_vol_on_price = self.calc_left_volume_on_price(price, 'B')
                if bid_vol_on_price > 0:  # 买量不是0 reverse=True
                    self.bid_prices_queue_decr.append(price)
                else:
                    self.bid_prices_queue_decr.append(self.last_price)
            if price >= self.last_price:
                ask_vol_on_price = self.calc_left_volume_on_price(price, 'S')
                if ask_vol_on_price > 0:  # 卖量不是0
                    self.ask_prices_queue_incr.append(price)
                else:
                    self.ask_prices_queue_incr.append(self.last_price)
        self.bid_prices_queue_decr.sort(reverse=True)
        self.ask_prices_queue_incr.sort()

    # @logger.catch
    def try_remove_order(self, orderobj):
        # return
        price = orderobj.price
        order_id = orderobj.norder
        # print (price , order_id , orderobj.status)
        if (orderobj.status == 'complete' or orderobj.status == 'cancel') and \
                orderobj.left_volume <= 0:
            del self.all_orders[order_id]
            del self.orderbook_on_prices[price][order_id]
            if len(self.orderbook_on_prices[price]) == 0:
                del self.orderbook_on_prices[price]
                self.update_prices_queue()

    # @logger.catch
    def match_best_price(self, ex, lclose_price: float = 0):
        self.last_price = lclose_price
        if self.orderbook_on_prices:
            bid_vol_dict = {}
            ask_vol_dict = {}
            max_vol_value = 0
            min_price_dict = {}
            all_prices = self.orderbook_on_prices.keys()
            max_vol_price = {}
            max_vol_value_last = 0
            max_vol_value_list = []
            for price in all_prices:
                bid_vol_value_sum = 0
                ask_vol_value_sum = 0
                for k in self.orderbook_on_prices.keys():
                    if k >= price:
                        # bid
                        bid_vol_on_price = self.calc_left_volume_on_price(k, 'B')
                        if bid_vol_on_price > 0:
                            bid_vol_value_sum += bid_vol_on_price
                            bid_vol_dict[price] = bid_vol_value_sum
                    if k <= price:
                        # ask
                        ask_vol_on_price = self.calc_left_volume_on_price(k, 'S')
                        if ask_vol_on_price > 0:
                            ask_vol_value_sum += ask_vol_on_price
                            ask_vol_dict[price] = ask_vol_value_sum
                min_vol_value = min([bid_vol_value_sum, ask_vol_value_sum])
                min_price_dict[price] = min_vol_value
                if min_vol_value > max_vol_value:
                    max_vol_value = min_vol_value
                    if max_vol_price:
                        last_val, max_vol_value_last = max_vol_price.popitem()
                    if not max_vol_price.get(max_vol_value):
                        max_vol_price[max_vol_value] = [price]
                elif min_vol_value == max_vol_value:
                    if max_vol_price.get(min_vol_value, None):
                        max_vol_price[min_vol_value] = [price]
                    else:
                        max_vol_price[min_vol_value].append(price)

            if min_price_dict:
                last_price = max(min_price_dict.items(), key=lambda x: x[1])
                vol_value_max = last_price[1]
                multiple_values = []
                for k, v in min_price_dict.items():
                    if vol_value_max == v:
                        multiple_values.append(k)
                if vol_value_max != 0:
                    if ex == 1 or ex == 'sh':
                        self.last_price = round(np.median(multiple_values), 2)
                    elif ex == 2 or ex == 'sz':
                        self.last_price = min(multiple_values, key=lambda x: abs(x - lclose_price))

    def get_tick_direction(self, new_price):
        if new_price > self.last_price:
            return "uptick"
        elif new_price < self.last_price:
            return "downtick"
        else:
            return "leveltick"

    # 市价
    # @logger.catch
    def get_current_price(self, kind, bs_type):
        try:
            if (kind == '1' and bs_type == 'B'):  # 买单，对方最优
                return self.ask_prices_queue_incr[0]
            elif (kind == 'U' and bs_type == 'B'):  # 买单，本方最优
                return self.bid_prices_queue_decr[0]
            elif (kind == '1' and bs_type == 'S'):  # 卖单，对方最优
                return self.bid_prices_queue_decr[0]
            elif (kind == 'U' and bs_type == 'S'):  # 卖单，本方最优
                return self.ask_prices_queue_incr[0]
            return 0.0
        except:
            pass

    # @logger.catch
    def update_ohlc(self, last_price):
        self.last_price = last_price
        if self.last_price > self.high_price:
            self.high_price = self.last_price
        if self.last_price < self.low_price:
            self.low_price = self.last_price
        self.close_price = self.last_price

    # @logger.catch
    def new_ohlc_bar(self):
        self.open_price = self.last_price
        self.high_price = self.last_price
        self.low_price = self.last_price
        self.close_price = self.last_price

    # @logger.catch
    def on_order(self, new_order, is_last=False):
        if new_order is not None:
            if isinstance(new_order, pd.DataFrame):
                new_order = new_order.to_list()
            exchange = new_order[3]
            if exchange == 1:  # sh
                self.on_order_sh(new_order, is_last)
            elif exchange == 2:  # sz
                self.on_order_sz(new_order, is_last)

    def on_transaction(self, new_trans, is_last=False):
        if new_trans is not None:
            if isinstance(new_trans, pd.DataFrame):
                new_trans = new_trans.to_list()
        self.cached_trans.append(new_trans)
        self.cached_trans = list(filter(lambda x: x is not None, self.cached_trans))

        for i in range(len(self.cached_trans)):
            trans = self.cached_trans[i]
            exchange = trans[3]
            processed = False
            if exchange == 1:  # sh
                processed = self.on_transaction_sh(trans, is_last)
            elif exchange == 2:  # sz
                processed = self.on_transaction_sz(trans, is_last)
            if processed == True:
                self.cached_trans[i] = None  # set to None to clear this order in the entry

        self.update_prices_queue()  # possibily slow..
        return len(self.cached_trans)

    # @logger.catch
    def on_order_sh(self, order, is_last=False):
        kind = chr(order[8])
        bs_type = chr(order[9])
        # order_id = int(order[5])
        order_id = int(order[11])
        price = float(order[6])
        volume = int(order[7])
        pd_dt = tdfdt_to_pddt(order[0], order[2])
        self.last_update_datetime = pd_dt.timestamp()
        self.last_order_datetime = pd_dt.timestamp()

        if kind == 'A':  # new order
            # update orderbook, dynamic
            orderobj = Order(bs_type, kind, pd_dt, order_id, price, volume)
            # a new price, init order queue and dop.
            if self.orderbook_on_prices.get(price) is None:
                self.orderbook_on_prices[price] = {}
                self.update_prices_queue()
            if self.dynamic_on_prices.get(price) is None:
                self.dynamic_on_prices[price] = DynamicOnPrice(price)
            # add order to order queue.
            self.orderbook_on_prices[price][order_id] = orderobj
            # add order to a lookup map.
            self.all_orders[order_id] = orderobj
            # update DOP
            self.dynamic_on_prices[price].put_order(pd_dt, volume, bs_type)

        elif kind == 'D':  # delete/cancel order
            cancel_orderid = order_id
            cancelorderobj = self.all_orders.get(cancel_orderid)
            if cancelorderobj is not None:
                # cancel lob order
                cancelorderobj.cancel_order(pd_dt, volume)
                # cancel DOP
                self.dynamic_on_prices[cancelorderobj.price].cancel_order(pd_dt, volume, cancelorderobj.bs_type)
                # self.cached_trans[i] = None
                self.try_remove_order(cancelorderobj)

        return True

    # @logger.catch
    def on_transaction_sh(self, trans, is_last=False):

        ts_type = trans[11]
        pd_dt = tdfdt_to_pddt(trans[0], trans[2])
        index = trans[5]
        price = float(trans[6])
        volume = int(trans[7])
        turnover = float(trans[8])
        bsflag = chr(trans[9])
        askorder = int(trans[12])
        bidorder = int(trans[13])

        # update orders
        self.last_update_datetime = pd_dt.timestamp()
        self.last_trans_datetime = pd_dt.timestamp()

        trans_lead = False
        if self.last_trans_datetime > self.last_order_datetime and (not is_last):
            trans_lead = True
        if ts_type == 0:  # 成交
            askorderobj = self.all_orders.get(askorder)
            bidorderobj = self.all_orders.get(bidorder)

            if (askorderobj is None or bidorderobj is None) and trans_lead:
                return False

            if askorderobj is not None:
                # confirm lob order
                # print ("askorderobj confirm_transaction " + str(volume) + "  " + str(price))
                askorderobj.confirm_transaction(pd_dt, volume, price)
                self.try_remove_order(askorderobj)
            if bidorderobj is not None:
                # print ("bidorderobj confirm_transaction " + str(volume) + "  " + str(price))
                bidorderobj.confirm_transaction(pd_dt, volume, price)
                self.try_remove_order(bidorderobj)

            # confirm DOP

            if self.dynamic_on_prices.get(price) is None:
                self.dynamic_on_prices[price] = DynamicOnPrice(price)
            self.dynamic_on_prices[price].confirm_transaction(pd_dt, volume, turnover, bsflag,
                                                              self.get_tick_direction(price))

            # update global vars
            self.update_ohlc(price)
            # clean
            return True
        return False

    # @logger.catch
    def on_order_sz(self, order, is_last=False):
        kind = chr(order[8])
        bs_type = chr(order[9])
        order_id = int(order[5])
        price = float(order[6])
        volume = int(order[7])
        pd_dt = tdfdt_to_pddt(order[0], order[2])

        # fix price

        if kind == '1' or kind == 'U':
            if bs_type == 'B' or bs_type == 'S':
                if self.get_current_price(kind, bs_type) is not None:
                    price = self.get_current_price(kind, bs_type)
                else:
                    price = self.last_price

        # update orderbook, dynamic
        self.last_update_datetime = pd_dt.timestamp()
        self.last_order_datetime = pd_dt.timestamp()

        orderobj = Order(bs_type, kind, pd_dt, order_id, price, volume)

        # a new price, init order queue and dop.
        if self.orderbook_on_prices.get(price) is None:
            self.orderbook_on_prices[price] = {}
            self.update_prices_queue()
        if self.dynamic_on_prices.get(price) is None:
            self.dynamic_on_prices[price] = DynamicOnPrice(price)

        # add order to order queue.
        self.orderbook_on_prices[price][order_id] = orderobj
        # add order to a lookup map.
        self.all_orders[order_id] = orderobj

        # update DOP
        self.dynamic_on_prices[price].put_order(pd_dt, volume, bs_type)

        return True

    # @logger.catch
    def on_transaction_sz(self, trans, is_last=False):
        ts_type = chr(trans[11])
        pd_dt = tdfdt_to_pddt(trans[0], trans[2])
        index = trans[5]
        price = float(trans[6])
        volume = int(trans[7])
        turnover = float(trans[8])
        bsflag = chr(trans[9])
        askorder = int(trans[12])
        bidorder = int(trans[13])

        # update orders
        self.last_update_datetime = pd_dt.timestamp()
        self.last_trans_datetime = pd_dt.timestamp()
        if ts_type == '0':  # 成交
            askorderobj = self.all_orders.get(askorder)
            bidorderobj = self.all_orders.get(bidorder)
            if (askorderobj is not None and bidorderobj is not None):
                # confirm lob order
                askorderobj.confirm_transaction(pd_dt, volume, price)
                bidorderobj.confirm_transaction(pd_dt, volume, price)
                # confirm DOP
                if self.dynamic_on_prices.get(price) is None:
                    self.dynamic_on_prices[price] = DynamicOnPrice(price)
                self.dynamic_on_prices[price].confirm_transaction(pd_dt, volume, turnover, bsflag,
                                                                  self.get_tick_direction(price))
                self.update_ohlc(price)
                # clean
                self.try_remove_order(askorderobj)
                self.try_remove_order(bidorderobj)
                return True
        elif ts_type == 'C':
            cancel_orderid = askorder if self.all_orders.get(askorder) is not None else bidorder
            cancelorderobj = self.all_orders.get(cancel_orderid)
            if cancelorderobj is not None:
                # cancel lob order
                cancelorderobj.cancel_order(pd_dt, volume)
                # cancel DOP
                self.dynamic_on_prices[cancelorderobj.price].cancel_order(pd_dt, volume, cancelorderobj.bs_type)
                self.try_remove_order(cancelorderobj)
                return True
        return False


class DynamicOnPrice:  # DOP, dynamic on a price
    MATRICS = ["actbuy", "actsell", "actunk", \
               "uptick", "downtick", "lvtick", \
               "aop", "aoc", \
               "bop", "boc"]
    DATATYPES = ["cnt", "vol", "amt"]
    SIZECLASS = ["sb", "bg", "md", "sm"]

    def __init__(self, price):

        # const
        self.price = price

        self.data = {}

        for metric in DynamicOnPrice.MATRICS:
            for datetype in DynamicOnPrice.DATATYPES:
                # overall
                property_name = "%s_%s" % (metric, datetype)
                # print(property_name)
                if datetype == "amt":
                    # setattr(self,property_name ,0.0)
                    self.data[property_name] = 0.0
                else:
                    # setattr(self, property_name,0)
                    self.data[property_name] = 0
                for sizeclass in DynamicOnPrice.SIZECLASS:
                    # with sizeclass
                    property_name = "%s_%s_%s" % (metric, datetype, sizeclass)
                    # print(property_name)
                    if datetype == "amt":
                        # setattr(self, property_name,0.0)
                        self.data[property_name] = 0.0
                    else:
                        # setattr(self, property_name,0)
                        self.data[property_name] = 0

        # statically defined vars
        self.update_ts = None  # update by using order and transaction
        self.trans_cnt = 0
        self.trans_vol = 0
        self.trans_amt = 0.0

    @staticmethod
    def List_Data_Names():
        lnames = []
        for metric in DynamicOnPrice.MATRICS:
            for datetype in DynamicOnPrice.DATATYPES:
                lnames.append("%s_%s" % (metric, datetype))
                for sizeclass in DynamicOnPrice.SIZECLASS:
                    lnames.append("%s_%s_%s" % (metric, datetype, sizeclass))
        lnames.extend(['trans_cnt', 'trans_vol', 'trans_amt', "update_ts"])
        return lnames

    def list_data(self):
        ldata = []
        for metric in DynamicOnPrice.MATRICS:
            for datetype in DynamicOnPrice.DATATYPES:
                ldata.append(self.data["%s_%s" % (metric, datetype)])
                for sizeclass in DynamicOnPrice.SIZECLASS:
                    ldata.append(self.data["%s_%s_%s" % (metric, datetype, sizeclass)])
        ldata.extend([self.trans_cnt, self.trans_vol, \
                      self.trans_amt, self.update_ts])
        return (self.price, ldata)

    def calc_sizeclass(self, volume, amount):
        if volume > 500000 or amount > 1000000.0:
            return "sb"
        if volume >= 100000 or amount >= 200000.0:
            return "bg"
        if volume >= 20000 or amount >= 40000.0:
            return "md"
        return "sm"

    def put_order(self, order_datetime, volume, bs_type):
        amount = self.price * volume
        sizeclass = self.calc_sizeclass(volume, amount)
        # bs_type == 'B' or bs_type == 'S'
        metric = "bop" if bs_type == 'B' else "aop"

        self.data["%s_cnt_%s" % (metric, sizeclass)] += 1
        self.data["%s_vol_%s" % (metric, sizeclass)] += volume
        self.data["%s_amt_%s" % (metric, sizeclass)] += amount

        self.data["%s_cnt" % (metric)] += 1
        self.data["%s_vol" % (metric)] += volume
        self.data["%s_amt" % (metric)] += amount
        self.update_ts = order_datetime

        '''
        ori_count = getattr(self,"%s_cnt_%s"%(metric,sizeclass))
        setattr(self,"%s_cnt_%s"%(metric,sizeclass),ori_count + 1)
        ori_volume = getattr(self,"%s_vol_%s"%(metric,sizeclass))
        setattr(self,"%s_vol_%s"%(metric,sizeclass),ori_volume + volume)
        ori_amount = getattr(self,"%s_amt_%s"%(metric,sizeclass))
        setattr(self,"%s_amt_%s"%(metric,sizeclass),ori_amount + amount)

        #overall
        ori_count = getattr(self,"%s_cnt"%(metric))
        setattr(self,"%s_cnt"%(metric),ori_count + 1)
        ori_volume = getattr(self,"%s_vol"%(metric))
        setattr(self,"%s_vol"%(metric),ori_volume + volume)
        ori_amount = getattr(self,"%s_amt"%(metric))
        setattr(self,"%s_amt"%(metric),ori_amount + amount)
        '''

        '''
        if bs_type == 'B':
            self.bid_order_put_count += 1
            self.bid_order_put_volume += volume
        elif bs_type == 'S':
            self.ask_order_put_count += 1
            self.ask_order_put_volume += volume
        self.update_ts = order_datetime
        '''

    def cancel_order(self, order_datetime, volume, bs_type):

        amount = self.price * volume
        sizeclass = self.calc_sizeclass(volume, amount)
        # bs_type == 'B' or bs_type == 'S'
        metric = "boc" if bs_type == 'B' else "aoc"

        self.data["%s_cnt_%s" % (metric, sizeclass)] += 1
        self.data["%s_vol_%s" % (metric, sizeclass)] += volume
        self.data["%s_amt_%s" % (metric, sizeclass)] += amount

        self.data["%s_cnt" % (metric)] += 1
        self.data["%s_vol" % (metric)] += volume
        self.data["%s_amt" % (metric)] += amount
        self.update_ts = order_datetime

    def confirm_transaction(self, trans_datetime, volume, turnover, bsflag, tick_direction):
        amount = turnover
        sizeclass = self.calc_sizeclass(volume, amount)
        metric = ""
        if bsflag == "B":
            metric = "actbuy"
        elif bsflag == 'S':
            metric = "actsell"
        else:
            metric = "actunk"

        self.data["%s_cnt_%s" % (metric, sizeclass)] += 1
        self.data["%s_vol_%s" % (metric, sizeclass)] += volume
        self.data["%s_amt_%s" % (metric, sizeclass)] += amount

        self.data["%s_cnt" % (metric)] += 1
        self.data["%s_vol" % (metric)] += volume
        self.data["%s_amt" % (metric)] += amount

        # static
        self.update_ts = trans_datetime
        self.trans_cnt += 1
        self.trans_vol += volume
        self.trans_amt += amount

        ########################################
        # up-down tick
        if tick_direction == 'uptick':
            metric = "uptick"
        elif tick_direction == "downtick":
            metric = "downtick"
        elif tick_direction == "leveltick":
            metric = "lvtick"

        self.data["%s_cnt_%s" % (metric, sizeclass)] += 1
        self.data["%s_vol_%s" % (metric, sizeclass)] += volume
        self.data["%s_amt_%s" % (metric, sizeclass)] += amount

        self.data["%s_cnt" % (metric)] += 1
        self.data["%s_vol" % (metric)] += volume
        self.data["%s_amt" % (metric)] += amount

        '''
        if bsflag == 'B':
            self.actbuy_count += 1
            self.actbuy_volume += volume
            self.actbuy_turnover += turnover
        elif bsflag == 'S':
            self.actsell_count += 1
            self.actsell_volume += volume
            self.actsell_turnover += turnover
        elif bsflag == ' ':
            self.actunknown_count += 1
            self.actunknown_volume += volume
            self.actunknown_turnover += turnover


        self.update_ts = trans_datetime
        self.transaction_count += 1
        self.transaction_volume += volume
        self.transaction_turnover += turnover

        if tick_direction == 'uptick':
            self.uptick_count += 1
            self.uptick_volume += volume
            self.uptick_turnover += turnover
        elif tick_direction == "downtick":
            self.downtick_count += 1
            self.downtick_volume += volume
            self.downtick_turnover += turnover
        elif tick_direction == "leveltick":
            self.leveltick_count += 1
            self.leveltick_volume += volume
            self.leveltick_turnover += turnover
        '''


# the snapshot_ts is a manunlly constructed ts. aligned with second
# return ts is the lob update ts
# @logger.catch
def snapshot_generator_classic(lob, snapshot_ts):
    # print ('taking snapshot_classic ' + str(lob.last_update_datetime) )
    # snapshot_ts = lob.last_update_datetime
    # if snapshot_ts is None:
    #    snapshot_ts = lob.last_update_datetime.floor(freq='S')

    asks_vol = []
    bids_vol = []

    asks_value = []
    bids_value = []

    for k, v in lob.orderbook_on_prices.items():
        av = reduce(
            lambda x, value: x + value.left_volume if value.bs_type == 'S' else 0,
            v.values(), 0)
        asks_vol.append((k, av))
        asks_value.append((k, av * float(k)))

        bv = reduce(
            lambda x, value: x + value.left_volume if value.bs_type == 'B' else 0,
            v.values(), 0)
        bids_vol.append((k, bv))
        bids_value.append((k, bv * float(k)))
    # print (full_dict)
    dops = []
    for k, v in lob.dynamic_on_prices.items():
        dops.append(v.list_data())

    asks_vol_dict = dict(asks_vol)
    bids_vol_dict = dict(bids_vol)
    asks_value_dict = dict(asks_value)
    bids_value_dict = dict(bids_value)
    dops_dict = dict(dops)

    full_dict = asks_vol_dict
    full_merge_dict(full_dict, bids_vol_dict)
    full_merge_dict(full_dict, asks_value_dict)
    full_merge_dict(full_dict, bids_value_dict)
    full_merge_dict(full_dict, dops_dict)

    # print(dops_dict)
    col_names = ['price', 'ask_vol', 'bid_vol', 'ask_amt', 'bid_amt']
    col_names.extend(DynamicOnPrice.List_Data_Names())
    # fix if the price has no bid or ask volume
    for k, v in full_dict.items():
        full_dict[k] = pad_or_truncate(v, len(col_names) - 1, False)

    df_on_prices = pd.DataFrame([[key, *var] for (key, var) in full_dict.items()],
                                columns=col_names)
    # print (df)
    # df.to_csv('test.csv')
    df_on_prices['snapshot_ts'] = snapshot_ts

    df_on_lob = pd.DataFrame([[
        snapshot_ts,
        lob.last_price,
        lob.open_price,
        lob.high_price,
        lob.low_price,
        lob.close_price,
        lob.ask_prices_queue_incr,
        lob.bid_prices_queue_decr
    ]], columns=['snapshot_ts', 'last_price', 'open_price', 'high_price', 'low_price', 'close_price', \
                 'ask_prices', 'bid_prices'])

    return df_on_prices, df_on_lob, snapshot_ts  # lob.last_update_datetime

# check if it needs to generate snapshot, if so, return []ts for the new snapshots, otherwise , return None

# @logger.catch
def snapshot_time_checker(last_snapshot_ts, newdata_ts, time_interval=3):
    # print(last_snapshot_ts, newdata_ts)
    if last_snapshot_ts is None:
        return [newdata_ts.floor(freq='S')]
    last_snapshot_ts_floor = last_snapshot_ts.floor(freq='S')
    newdata_ts_floor = newdata_ts.floor(freq='S')

    if newdata_ts_floor < last_snapshot_ts_floor:
        return []
    timespan = (newdata_ts_floor - last_snapshot_ts_floor).seconds
    ret_ts = []

    for i in range(0, timespan + 1 - time_interval, time_interval):
        last_snapshot_ts_floor += pd.Timedelta(seconds=time_interval)
        if is_hour_between(last_snapshot_ts_floor, '09:25:03 +0800', '09:29:57 +0800') or is_hour_between(
                last_snapshot_ts_floor, '11:30:03 +0800', '12:59:57 +0800'):
            continue
        ret_ts.append(last_snapshot_ts_floor)
    return ret_ts

# @logger.catch
def sp_sl_to_influx(instrument, sp, sl, influx_writer, bucket=None, org=None):
    # print(sp, sl)
    dt = sl.loc[0, 'snapshot_ts']
    points = []
    points.append(
        Point("sl_last_price").tag("instrument", instrument).field("value", float(sl.loc[0, 'last_price'])).time(dt))
    points.append(
        Point("sl_open_price").tag("instrument", instrument).field("value", float(sl.loc[0, 'open_price'])).time(dt))
    points.append(
        Point("sl_high_price").tag("instrument", instrument).field("value", float(sl.loc[0, 'high_price'])).time(dt))
    points.append(
        Point("sl_low_price").tag("instrument", instrument).field("value", float(sl.loc[0, 'low_price'])).time(dt))
    points.append(
        Point("sl_close_price").tag("instrument", instrument).field("value", float(sl.loc[0, 'close_price'])).time(dt))

    str_ask_prices = ['{:.2f}'.format(x) for x in sl.loc[0, 'ask_prices']]
    points.append(
        Point("sl_ask_prices").tag("instrument", instrument).field("value", ','.join(str_ask_prices)).time(dt))
    points.append(
        Point("sl_ask_prices_depth").tag("instrument", instrument).field("value", len(str_ask_prices)).time(dt))
    str_bid_prices = ['{:.2f}'.format(x) for x in sl.loc[0, 'bid_prices']]
    points.append(
        Point("sl_bid_prices").tag("instrument", instrument).field("value", ','.join(str_bid_prices)).time(dt))
    points.append(
        Point("sl_bid_prices_depth").tag("instrument", instrument).field("value", len(str_bid_prices)).time(dt))

    int_keys = ['ask_vol', 'bid_vol']
    float_keys = ['ask_amt', 'bid_amt']
    for metric in DynamicOnPrice.MATRICS:
        for datetype in DynamicOnPrice.DATATYPES:

            if datetype == "vol" or datetype == "cnt":
                int_keys.append("%s_%s" % (metric, datetype))
            if datetype == "amt":
                float_keys.append("%s_%s" % (metric, datetype))

            for sizeclass in DynamicOnPrice.SIZECLASS:
                if datetype == "vol" or datetype == "cnt":
                    int_keys.append("%s_%s_%s" % (metric, datetype, sizeclass))
                if datetype == "amt":
                    float_keys.append("%s_%s_%s" % (metric, datetype, sizeclass))

    '''
    int_keys = ['ask_volume', 'bid_volume', 'ask_value', 'bid_value',
                'actbuy_count', 'actbuy_volume',
                'actsell_count', 'actsell_volume',
                'actunknown_count', 'actunknown_volume',

                'uptick_count', 'uptick_volume',
                'downtick_count', 'downtick_volume',
                'leveltick_count', 'leveltick_volume',

                'transaction_count', 'transaction_volume',
                'ask_order_put_count', 'ask_order_put_volume', 'ask_order_cancel_count',
                'ask_order_cancel_volume', 'bid_order_put_count', 'bid_order_put_volume',
                'bid_order_cancel_count', 'bid_order_cancel_volume']

    float_keys = ['actbuy_turnover', 'actsell_turnover', 'actunknown_turnover', 'transaction_turnover',
                  'uptick_turnover', 'downtick_turnover', 'leveltick_turnover']
    '''
    # lob
    for key in int_keys:
        points.append(Point("sl_" + key)
                      .tag("instrument", instrument)
                      .field("value", int(sp[key].sum()))
                      .time(dt))

    for key in float_keys:
        points.append(Point("sl_" + key)
                      .tag("instrument", instrument)
                      .field("value", float(sp[key].sum()))
                      .time(dt))

    for i, r in sp.iterrows():
        price = '{:.2f}'.format(r['price'])

        for key in int_keys:
            if key not in set(['ask_vol', 'bid_vol']):
                continue
            points.append(Point("sp_" + key)
                          .tag("instrument", instrument)
                          .tag("price", price)
                          .field("value", int(r[key]))
                          .time(dt))

        for key in float_keys:
            if key not in set(['ask_amt', 'bid_amt']):
                continue
            points.append(Point("sp_" + key)
                          .tag("instrument", instrument)
                          .tag("price", price)
                          .field("value", float(r[key]))
                          .time(dt))

    async_result = influx_writer.write(bucket, org, points)
    # return async_result

def int_ex_to_str(ex):
    if ex == 1:
        return 'sh'
    elif ex == 2:
        return 'sz'
@logger.catch
def get_yaml_data(yaml_file):
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    data = yaml.load(file_data, Loader=yaml.Loader)
    return data


class Load_configuration:
    def __init__(self, config_path):
        self.current_path = os.path.dirname(os.path.abspath(__file__))
        self.yaml_path = os.path.join(self.current_path, config_path)
        self.yaml_data = get_yaml_data(self.yaml_path)
        self.topics = self.yaml_data.get("kafka_server", {}).get("topics")
        self.bootstrap_servers = self.yaml_data.get("kafka_server", {}).get("bootstrap_servers")
        self.date_time = self.yaml_data.get("date_time")
        self.token = self.yaml_data.get("influxdb", {}).get("token")
        self.org = self.yaml_data.get("influxdb", {}).get("org")
        self.bucket = self.yaml_data.get("influxdb", {}).get("bucket")
        self.url = self.yaml_data.get("influxdb", {}).get("url")
        self.time_interval = self.yaml_data.get("time_interval", 3)
        self.kafka_all_data_flag = self.yaml_data.get("seek_to_beginning", False)
        self.oracle_config = {
            'user': self.yaml_data.get("oracle", {}).get("user"),
            'password': self.yaml_data.get("oracle", {}).get("password"),
            'host': self.yaml_data.get("oracle", {}).get("host"),
            'port': self.yaml_data.get("oracle", {}).get("port"),
            'service_name': self.yaml_data.get("oracle", {}).get("service_name")
        }


@logger.catch
def kafka_to_gen_snapshots(topic, consumer=None, instruments=None, date_time=None, influx_writer=None, lc=None):
    date_str_list = []
    timestamp = set_datetime_offsets(consumer, topic, date_time=date_time)
    lclose_price_lookup_tab = get_lclose_price(lc.oracle_config)
    lob_map = {}
    snapshot_ts_map = {}
    code = None
    for msg in consumer:  # 迭代器，等待下一条消息
        key = msg.key.decode()
        # 获取消费者key值
        inst_code, ex_code = tuple(key.split("."))
        # 过滤 获取上海6开头和深圳3或者0开头的信息
        if (ex_code == "1" and inst_code.startswith('6')) \
                or (ex_code == "1" and inst_code.startswith('5')) \
                or ((ex_code == "2" and inst_code.startswith('3')) or (ex_code == "2" and inst_code.startswith('0'))):
            newdata_ts = None
            newdata_ts_f = None
            if msg.headers:
                # 获取headers中的k,v区分order和transaction的数据
                k_info, code = msg.headers[0]
                code = code.decode()
                # logger.debug(code)
            if code == "321":
                # 处理kafka protoc中order数据
                order_pb = protobuf_order_parse(msg.value)
                order_data_list = parse_pb(order_pb)
                # print(order_data_list)
                ex = order_data_list[3]  # 市场
                strex = int_ex_to_str(ex)
                instrument = order_data_list[4]  # 股票代码
                strdate = order_data_list[0]  # 日期
                if strdate not in date_str_list:
                    date_str_list.append(strdate)
                    if len(date_str_list) >= 2:
                        lclose_price_lookup_tab = get_lclose_price(lc.oracle_config)
                        lob_map = {}
                        snapshot_ts_map = {}
                        date_str_list = []
                        date_str_list.append(strdate)

                newdata_ts = format_tdftime(order_data_list)
                if not lob_map.get(key):
                    lob = LimitOrderBook(ex, instrument, strdate)
                    lob.on_order(order_data_list)
                    lob.on_transaction(None)
                    lob_map[key] = lob
                else:
                    lob_map.get(key).on_order(order_data_list)
                    lob_map.get(key).on_transaction(None)
            elif code == "94":
                # 处理kafka protoc中transaction数据
                transaction_pb = protobuf_transactions_parse(msg.value)
                transaction_data_list = parse_pb(transaction_pb)
                ex = transaction_data_list[3]
                strex = int_ex_to_str(ex)
                instrument = transaction_data_list[4]
                strdate = transaction_data_list[0]
                if strdate not in date_str_list:
                    date_str_list.append(strdate)
                    if len(date_str_list) >= 2:
                        lclose_price_lookup_tab = get_lclose_price(lc.oracle_config)
                        lob_map = {}
                        snapshot_ts_map = {}
                        date_str_list = []
                        date_str_list.append(strdate)
                newdata_ts = format_tdftime(transaction_data_list)
                if not lob_map.get(key):
                    lob = LimitOrderBook(ex, instrument, strdate)
                    lob.on_order(None)
                    lob.on_transaction(transaction_data_list)
                    lob_map[key] = lob
                else:
                    lob_map.get(key).on_order(None)
                    lob_map.get(key).on_transaction(transaction_data_list)

            if code == "321" or code == "94":
                newdata_ts_floor = newdata_ts.floor(freq='S')
                snapshot_ts_check_res = snapshot_time_checker(snapshot_ts_map.get(key), newdata_ts, time_interval)
                if snapshot_ts_check_res is not None:
                    for s in snapshot_ts_check_res:
                        if is_hour_between(newdata_ts_floor, '09:15:00 +0800', '09:25:00 +0800'):
                            lob_map.get(key).match_best_price(int(ex_code),
                                                              lclose_price=lclose_price_lookup_tab.get(inst_code, 0))
                        sp, sl, snapshot_ts_map[key] = snapshot_generator_classic(lob_map.get(key), s)
                        lob_map.get(key).new_ohlc_bar()
                        logger.info('took snapshot @ ' + str(snapshot_ts_map.get(key)) + key)
                        print('took snapshot @ ' + str(snapshot_ts_map.get(key)) + key)
                        # print(sp)
                        # print(sl)
                        # print(snapshot_ts_map)
                        if snapshot_ts_map.get(key) is not None:
                            pass
                            # snapshot_on_prices[snapshot_ts_map.get(key)] = sp
                            # snapshot_on_lob[snapshot_ts_map.get(key)] = sl
                            # sp_sl_to_influx(strex + '.' + instrument, sp, sl, influx_writer, bucket=bucket, org=org)

@logger.catch
def format_tdftime(data_list):
    merged_datetime = data_list[0] + data_list[2].zfill(9)
    formated_datetime = merged_datetime[0:4] + '-' + \
                        merged_datetime[4:6] + '-' + \
                        merged_datetime[6:8] + '-' + \
                        merged_datetime[8:10] + '-' + \
                        merged_datetime[10:12] + '-' + \
                        merged_datetime[12:14] + '-' + \
                        merged_datetime[14:17] + ' +0800'

    return pd.to_datetime(formated_datetime, format="%Y-%m-%d-%H-%M-%S-%f %z", utc=True)

@logger.catch
def set_datetime_offsets(consumer, topic, date_time=None, kafka_all_data_flag=False):
    available_partitions = consumer.partitions_for_topic(topic)
    if not available_partitions:
        raise Exception("topic not exist.")
    '''
    offset=0, timestamp=1648430483893
    offset=484, timestamp=1648430484908
    offset=499, timestamp=1648430484932
    '''
    timestamp = int(local_time_timestamp(date_time=date_time)) * 1000
    dtps = {}
    # offsetAndTimestamp = {}
    tps = [TopicPartition(topic, tp) for tp in range(len(available_partitions))]
    for tp in tps:
        dtps[tp] = timestamp
    offsets = consumer.offsets_for_times(dtps)
    consumer.assign(offsets)
    for tp in consumer.assignment():
        offsetAndTimestamp = offsets[tp]
        if not kafka_all_data_flag:
            if offsetAndTimestamp is not None:
                consumer.seek(tp, offsetAndTimestamp.offset)
            else:
                consumer.seek_to_end(tp)
                logger.warning("没有此时间戳下的数据...")
                logger.warning("seek_to_end...")
        else:
            consumer.seek_to_beginning(tp)
            logger.warning("数据从offset=0开始消费")
            logger.warning("seek_to_beginning...")

    return timestamp

@logger.catch
def multiproces_topics(topic, date_time=None, lc=None):
    consumer = KafkaConsumer(
        bootstrap_servers=lc.bootstrap_servers,
        group_id="quote_lob_" + get_datetime_str(),
        enable_auto_commit=False,
        auto_offset_reset='earliest',
    )
    influx_client, influx_writer = init_influx_client(url=lc.url, token=lc.token, org=lc.org)
    kafka_to_gen_snapshots(topic, date_time=date_time, influx_writer=influx_writer, consumer=consumer, lc=lc)

@logger.catch
def init_influx_client(url=None, token=None, org=None):
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=ASYNCHRONOUS)
    logger.info("influxdb client initialized.")
    return client, write_api

@logger.catch
def start_multiproces_topics_run():
    lc = Load_configuration("config.yaml")
    topics = lc.topics
    topics = list(set(topics))
    with ProcessPoolExecutor(max_workers=len(topics)) as executor:
        for topic in topics:
            kargs = {"date_time": lc.date_time, "lc": lc}
            executor.submit(multiproces_topics, (topic), **kargs)
def main():
    start_multiproces_topics_run()
