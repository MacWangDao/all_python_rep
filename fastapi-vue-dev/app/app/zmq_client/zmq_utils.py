import os
import sys
import traceback

import zmq
# import msgcarrier_pb2, quote_pb2
from app.zmq_client import msgcarrier_pb2, quote_pb2
import datetime
import json
import yaml
import redis
from loguru import logger


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


def protobuf_msgcarrier_parse(response):
    msg = msgcarrier_pb2.MsgCarrier()
    msg.ParseFromString(response)
    msg_type = msg.type
    message_info = msg.message
    return msg_type, message_info


def protobuf_snapshot_parse(message_info):
    snapshot = quote_pb2.SnapShot()
    snapshot.ParseFromString(message_info)
    return snapshot


def protobuf_order_parse(message_info):
    order = quote_pb2.Order()
    order.ParseFromString(message_info)
    return order


def protobuf_transactions_parse(message_info):
    transactions = quote_pb2.Transactions()
    transactions.ParseFromString(message_info)
    transaction = transactions.items[0]
    return transaction


def redis_connection(host='127.0.0.1', port=6379):
    Pool = redis.ConnectionPool(host=host, port=port, max_connections=10, decode_responses=True)
    conn = redis.Redis(connection_pool=Pool)
    return conn


def redis_hmset(conn, name, mapping):
    try:
        conn.hset(name, mapping=mapping)
        logger.info(mapping)
    except Exception as e:
        traceback.print_exception()


def snapshot_merge_data(snapshot_data):
    trade_date = snapshot_data[0]
    trade_time = snapshot_data[1]
    exchange = snapshot_data[2]
    code = snapshot_data[3]
    lastprice = snapshot_data[5]
    high = snapshot_data[8]
    low = snapshot_data[9]
    volume = snapshot_data[10]
    highlimited = snapshot_data[12]
    lowlimited = snapshot_data[13]
    mapping = {"trade_date": trade_date, "trade_time": trade_time, "code": code, "exchange": exchange,
               "lastprice": lastprice,
               "high": high, "low": low, "volume": volume, "highlimited": highlimited, "lowlimited": lowlimited}
    return mapping


def zmq_conn(host="192.168.101.202", port=8046):
    uri = f"tcp://{host}:{port}"
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(uri)
    socket.setsockopt(zmq.SUBSCRIBE, ''.encode('utf-8'))  # 接收所有消息
    return socket


def parse_zmq_redis():
    redis_conn = redis_connection(host='127.0.0.1', port=6379)
    socket = zmq_conn(host="192.168.101.202", port=8046)
    while True:
        response = socket.recv()
        try:
            response = response.decode('utf-8')
        except UnicodeDecodeError as arg:
            msg = msgcarrier_pb2.MsgCarrier()
            msg.ParseFromString(response)
            msg_type = msg.type
            message_info = msg.message
            # print(msg_type)
            if msg_type == 26:
                # SNAPSHOT
                snapshot = protobuf_snapshot_parse(message_info)
                snapshot_data = parse_pb(snapshot)
                # print(snapshot_data)
                mapping = snapshot_merge_data(snapshot_data)
                name = mapping.get("code")
                redis_hmset(redis_conn, name, mapping)
                pass
            elif msg_type == 27:
                pass
            elif msg_type == 94:
                pass

        except Exception as e:
            traceback.print_exc()


def parse_pb(msg):
    if isinstance(msg, quote_pb2.Transaction):
        transaction_date = msg.date
        transaction_time = msg.time
        transaction_exchange = msg.exchange
        transaction_code = msg.code
        transaction_nIndex = msg.nIndex
        transaction_lastprice = msg.lastprice
        transaction_volume = msg.volume
        transaction_turnover = msg.turnover
        transaction_nBSFlag = msg.nBSFlag
        transaction_chOrderKind = msg.chOrderKind
        transaction_chFunctionCode = msg.chFunctionCode
        transaction_nAskOrder = msg.nAskOrder
        transaction_nBidOrder = msg.nBidOrder
        transaction_seqno = msg.seqno
        transaction_data_list = [
            str(transaction_date),
            transaction_time,
            msec_to_tdftime(transaction_time),
            transaction_exchange,
            transaction_code,
            transaction_nIndex,
            transaction_lastprice,
            transaction_volume,
            transaction_turnover,
            transaction_nBSFlag,
            transaction_chOrderKind,
            transaction_chFunctionCode,
            transaction_nAskOrder,
            transaction_nBidOrder,
            transaction_seqno
        ]
        return transaction_data_list
    elif isinstance(msg, quote_pb2.SnapShot):
        snapshot_date = msg.date
        snapshot_time = msg.time
        snapshot_exchange = msg.exchange
        snapshot_code = msg.code
        snapshot_status = msg.status
        snapshot_lastprice = msg.lastprice
        snapshot_prevclose = msg.prevclose
        snapshot_open = msg.open
        snapshot_high = msg.high
        snapshot_low = msg.low
        snapshot_volume = msg.volume
        snapshot_value = msg.value
        snapshot_highlimited = msg.highlimited
        snapshot_lowlimited = msg.lowlimited
        snapshot_niopv = msg.niopv
        snapshot_nyieldtomaturity = msg.nyieldtomaturity
        snapshot_numtrades = msg.numtrades
        snapshot_totalBidVol = msg.totalBidVol
        snapshot_totalAskVol = msg.totalAskVol
        snapshot_bidprices_list = msg.bidprices
        snapshot_bidvolumes_list = msg.bidvolumes
        snapshot_askprices_list = msg.askprices
        snapshot_askvolumes_list = msg.askvolumes
        snapshot_seqno = msg.seqno
        snapshot_datatype = msg.datatype
        snapshot_nWeightedAvgBidPrice = msg.nWeightedAvgBidPrice
        snapshot_nWeightedAvgAskPrice = msg.nWeightedAvgAskPrice
        snapshot_nSyl1 = msg.nSyl1
        snapshot_nSyl2 = msg.nSyl2
        snapshot_data_list = [
            snapshot_date,
            # snapshot_time,
            msec_to_tdftime(snapshot_time),
            snapshot_exchange,
            snapshot_code,
            snapshot_status,
            snapshot_lastprice,
            snapshot_prevclose,
            snapshot_open,
            snapshot_high,
            snapshot_low,
            snapshot_volume,
            snapshot_value,
            snapshot_highlimited,
            snapshot_lowlimited,
            snapshot_niopv,
            snapshot_nyieldtomaturity,
            snapshot_numtrades,
            snapshot_totalBidVol,
            snapshot_totalAskVol,
            snapshot_bidprices_list,
            snapshot_bidvolumes_list,
            snapshot_askprices_list,
            snapshot_askvolumes_list,
            snapshot_seqno,
            snapshot_datatype,
            snapshot_nWeightedAvgBidPrice,
            snapshot_nWeightedAvgAskPrice,
            snapshot_nSyl1,
            snapshot_nSyl2
        ]
        return snapshot_data_list
    elif isinstance(msg, quote_pb2.Order):
        order_date = msg.date
        order_time = msg.time
        # order_tdftime = msg.tdftime
        order_exchange = msg.exchange
        order_code = msg.code
        order_norder = msg.nOrder
        order_nprice = msg.nPrice
        order_nvolume = msg.nVolume
        order_chorderkind = msg.chOrderKind
        order_chfunctioncode = msg.chFunctionCode
        order_seqno = msg.seqno
        order_nOrderOriNo = msg.nOrderOriNo
        order_nBizIndex = msg.nBizIndex
        order_data_list = [
            str(order_date),
            order_time,
            msec_to_tdftime(order_time),
            # order_tdftime,
            order_exchange,
            order_code,
            order_norder,
            order_nprice,
            order_nvolume,
            order_chorderkind,
            order_chfunctioncode,
            order_seqno,
            order_nOrderOriNo,
            order_nBizIndex
        ]
        return order_data_list


def msec_to_tdftime(msec_time):
    sec = msec_time // 1000
    return str(((sec // 3600 * 10000) + (sec % 3600 // 60 * 100) + sec % 60) * 1000 + msec_time % 1000)


def get_yaml_data(yaml_file):
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    data = yaml.load(file_data, Loader=yaml.Loader)
    return data


def str_to_timestamp(date_str, format="%Y-%m-%d %H:%M:%S"):
    if date_str:
        timestamp = datetime.datetime.strptime(date_str, format).timestamp()
        return timestamp


def timestamp_to_str(timestamp, format="%Y-%m-%d %H:%M:%S"):
    if timestamp:
        return datetime.datetime.utcfromtimestamp(timestamp)


def local_time_timestamp(is_date=True, date_time=None):
    if date_time:
        return str_to_timestamp(date_time)
    now = datetime.datetime.now()
    if is_date:
        otherStyleTime = now.strftime("%Y-%m-%d")
        otherStyleTime = otherStyleTime + " 09:00:00"
    else:
        otherStyleTime = now.strftime("%Y-%m-%d %H:%M:%S")
    return str_to_timestamp(otherStyleTime)


def redis_test():
    import json
    redis_conn = redis_connection(host='127.0.0.1', port=6379)
    # mapping = {"dd": "22"}
    # redis_conn.hset("d3", mapping=mapping)
    # redis_conn.hset("d1", "name", "li")
    s = redis_conn.hgetall("600039")
    print(s)
    # redis_hmset(redis_conn, "d2", mapping)
