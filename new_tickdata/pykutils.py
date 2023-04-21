import os
import sys

# from dbutils.pooled_db import PooledDB

current_path = os.path.dirname(os.path.abspath(__file__))
father_path = os.path.dirname(current_path)
sys.path.append(father_path)
sys.path.append(os.path.join(father_path, "base"))
import zmq
import msgcarrier_pb2, quote_pb2
from enum import Enum, unique
import os
import numpy as np
import pandas as pd
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
from kafka import KafkaProducer, KafkaConsumer

import datetime
import json
import re
import yaml
import cx_Oracle as Oracle


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


@unique
class Trade(Enum):
    SNAPSHOT = 26
    ORDER = 27
    TRANSACTION = 94


class Parse_PB():
    def __init__(self):
        pass

    def parse_msgcarrier(self, msg):
        pass

    def parse_snapshot(self, msg):
        pass

    def parse_transaction(self, msg):
        pass

    def parse_order(self, msg):
        pass


def create_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = KafkaAdminClient(bootstrap_servers="192.168.101.212:9092", client_id='kafka01')
    topic_list = []
    if topic_name:
        topic_list.append(
            NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        admin_client.close()


def delete_topics(topic_name_list: list):
    admin_client = KafkaAdminClient(bootstrap_servers="192.168.101.212:9092", client_id='kafka01')
    if topic_name_list:
        admin_client.delete_topics(topic_name_list, timeout_ms=50)
        admin_client.close()


def get_topics():
    client = SimpleClient("192.168.101.212:9092", client_id='kafka01')
    return client.topics


def get_consumer_topics_():
    consumer = KafkaConsumer(
        bootstrap_servers=["192.168.101.212:9092", "192.168.101.213:9092", "192.168.101.214:9092"]
    )
    return consumer.topics()


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
    # print(type(transactions.items[0]))
    # print(transactions.items[0])


def parse_zmq():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:8047")
    socket.setsockopt(zmq.SUBSCRIBE, ''.encode('utf-8'))  # 接收所有消息
    while True:
        response = socket.recv()
        try:
            response = response.decode('utf-8')
            print(response)
        except UnicodeDecodeError as arg:
            # print('ERROR', arg)
            msg = msgcarrier_pb2.MsgCarrier()
            msg.ParseFromString(response)
            msg_type = msg.type
            message_info = msg.message
            print(msg_type)
            if msg_type == 26:
                snapshot = quote_pb2.SnapShot()
                snapshot.ParseFromString(message_info)
                # print(type(snapshot))
                # print(snapshot.date)
                # print(snapshot.bidprices)
                # print(snapshot.bidvolumes)
                # print(snapshot.askprices)
                # print(snapshot.askvolumes)
                # print(snapshot)
                # SNAPSHOT
                pass
            elif msg_type == 27:
                order = quote_pb2.Order()
                order.ParseFromString(message_info)
                print(type(order))
                print(order)
            elif msg_type == 94:
                # TRANSACTIONS
                transactions = quote_pb2.Transactions()
                transactions.ParseFromString(message_info)
                # print(type(transactions.items[0]))
                # print(transactions.items[0])
                pass

        except Exception as e:
            print(e)
            print(2)


def parse_zmq_pb_main():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:8047")
    socket.setsockopt(zmq.SUBSCRIBE, ''.encode('utf-8'))  # 接收所有消息
    producer = kafka_producer()
    while True:
        """
        response 26.2.159981 和 proto数据间隔接受
        """
        response = socket.recv()
        try:
            response = response.decode('utf-8')
        except UnicodeDecodeError as arg:
            msg = msgcarrier_pb2.MsgCarrier()
            msg.ParseFromString(response)
            msg_type, message_info = protobuf_msgcarrier_parse(response)
            if msg_type == 26:
                # SNAPSHOT
                snapshot = protobuf_snapshot_parse(message_info)
                # parse_pb(snapshot)
            elif msg_type == 321:
                # ORDER
                order = protobuf_order_parse(message_info)
                order_data = parse_pb(order)
                key = str(order_data[4]) + "." + str(order_data[3])
                key = key.encode("utf-8")
                value = message_info
                headers = [("o", "321".encode())]
                kafka_send(producer, topic="quote-dev-1", key=key, value=value, headers=headers)
            elif msg_type == 94:
                # TRANSACTIONS
                transactions = protobuf_transactions_parse(message_info)
                transactions_data = parse_pb(transactions)
                key = str(transactions_data[4]) + "." + str(transactions_data[3])
                key = key.encode("utf-8")
                value = message_info
                headers = [("7", "94".encode())]
                kafka_send(producer, topic="quote-dev-1", key=key, value=value, headers=headers)
        except Exception as e:
            print(e)


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
        # print("*************transaction***************")
        # print(transaction_data_list)
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
            snapshot_time,
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
        print(snapshot_data_list)
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
        # print("*************order***************")
        # print(order_data_list)
        return order_data_list


def msec_to_tdftime(msec_time):
    sec = msec_time // 1000
    return str(((sec // 3600 * 10000) + (sec % 3600 // 60 * 100) + sec % 60) * 1000 + msec_time % 1000)


def storage_path(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def data_to_csv():
    order_columns = ["date", "time", "tdftime", "exchange", "code", "norder", "nprice", "nvolume", "chorderkind",
                     "chfunctioncode", "seqno"]
    transaction_columns = ["date", "time", "tdftime", "exchange", "code", "nindex", "lastprice", "volume", "turnover",
                           "nbsflag", "chorderkind", "chfunctioncode", "naskorder", "nbidorder", "seqno"]
    snapshot_columns = ["date", "time", "tdftime", "exchange", "code", "status", "lastprice", "prevclose", "open",
                        "high",
                        "low", "volume", "value", "highlimited", "lowlimited", "settleprice", "prevsettleprice",
                        "delta",
                        "prevdelta", "iopeninterest", "prevopeninterest", "niopv", "nyieldtomaturity", "numtrades",
                        "totalbidvol", "totalaskvol", "bidprices0", "bidprices1", "bidprices2", "bidprices3",
                        "bidprices4",
                        "bidprices5", "bidprices6", "bidprices7", "bidprices8", "bidprices9", "bidvolumes0",
                        "bidvolumes1",
                        "bidvolumes2", "bidvolumes3", "bidvolumes4", "bidvolumes5", "bidvolumes6", "bidvolumes7",
                        "bidvolumes8", "bidvolumes9", "askprices0", "askprices1", "askprices2", "askprices3",
                        "askprices4",
                        "askprices5", "askprices6", "askprices7", "askprices8", "askprices9", "askvolumes0",
                        "askvolumes1",
                        "askvolumes2", "askvolumes3", "askvolumes4", "askvolumes5", "askvolumes6", "askvolumes7",
                        "askvolumes8", "askvolumes9", "seqno", "datatype", "vwap", "agregatevol", "ddrtiopv",
                        "nweightedavgbidprice", "nweightedavgaskprice", "nsyl1", "nsyl2", "moneyflow", "alpha", "beta"]


def kafka_producer():
    producer = KafkaProducer(bootstrap_servers=["192.168.101.212:9092", "192.168.101.213:9092", "192.168.101.214:9092"],
                             compression_type="lz4", key_serializer=None,
                             value_serializer=None)
    return producer


def kafka_send(producer, topic=None, key=None, value=None, headers=None, timestamp=None):
    future = producer.send(
        topic,
        key=key,
        value=value,
        headers=headers,
        timestamp=timestamp
    )
    producer.flush()

    # 获取发送记录的metadata
    try:
        record_metadata = future.get(timeout=10)
        print(record_metadata.offset)
        # print(record_metadata, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
    except KafkaError as e:
        print(e)


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


# current_path = os.path.abspath(".")
# yaml_path = os.path.join(current_path, "config.yaml")
# get_yaml_data(yaml_path)

class OraclePool:
    """
    1) 这里封装了一些有关oracle连接池的功能;
    2) sid和service_name，程序会自动判断哪个有值，
        若两个都有值，则默认使用service_name；
    3) 关于config的设置，注意只有 port 的值的类型是 int，以下是config样例:
        config = {
            'user':         'maixiaochai',
            'password':     'maixiaochai',
            'host':         '192.168.158.1',
            'port':         1521,
            'sid':          'maixiaochai',
            'service_name': 'maixiaochai'
        }
    """

    def __init__(self, config):
        """
        获得连接池
        :param config:      dict    Oracle连接信息
        """
        self.__pool = self.__get_pool(config)

    @staticmethod
    def __get_pool(config):
        """
        :param config:        dict    连接Oracle的信息
        ---------------------------------------------
        以下设置，根据需要进行配置
        maxconnections=6,   # 最大连接数，0或None表示不限制连接数
        mincached=2,        # 初始化时，连接池中至少创建的空闲连接。0表示不创建
        maxcached=5,        # 连接池中最多允许的空闲连接数，很久没有用户访问，连接池释放了一个，由6个变为5个，
                            # 又过了很久，不再释放，因为该项设置的数量为5
        maxshared=0,        # 在多个线程中，最多共享的连接数，Python中无用，会最终设置为0
        blocking=True,      # 没有闲置连接的时候是否等待， True，等待，阻塞住；False，不等待，抛出异常。
        maxusage=None,      # 一个连接最多被使用的次数，None表示无限制
        setession=[],       # 会话之前所执行的命令, 如["set charset ...", "set datestyle ..."]
        ping=0,             # 0  永远不ping
                            # 1，默认值，用到连接时先ping一下服务器
                            # 2, 当cursor被创建时ping
                            # 4, 当SQL语句被执行时ping
                            # 7, 总是先ping
        """
        dsn = None
        host, port = config.get('host'), config.get('port')

        if 'service_name' in config:
            dsn = Oracle.makedsn(host, port, service_name=config.get('service_name'))

        elif 'sid' in config:
            dsn = Oracle.makedsn(host, port, sid=config.get('sid'))

        pool = PooledDB(
            Oracle,
            mincached=5,
            maxcached=10,
            user=config.get('user'),
            password=config.get('password'),
            dsn=dsn
        )

        return pool

    def __get_conn(self):
        """
        从连接池中获取一个连接，并获取游标。
        :return: conn, cursor
        """
        conn = self.__pool.connection()
        cursor = conn.cursor()

        return conn, cursor

    @staticmethod
    def __reset_conn(conn, cursor):
        """
        把连接放回连接池。
        :return:
        """
        cursor.close()
        conn.close()

    def __execute(self, sql, args=None):
        """
        执行sql语句
        :param sql:     str     sql语句
        :param args:    list    sql语句参数列表
        :param return:  cursor
        """
        conn, cursor = self.__get_conn()

        if args:
            cursor.execute(sql, args)
        else:
            cursor.execute(sql)

        return conn, cursor

    def fetch_all(self, sql, args=None):
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchall()
        self.__reset_conn(conn, cursor)

        return result

    def fetch_one(self, sql, args=None):
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchone()
        self.__reset_conn(conn, cursor)

        return result

    def execute_sql(self, sql, args=None):
        """
        执行SQL语句。
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        conn.commit()
        self.__reset_conn(conn, cursor)

    def __del__(self):
        """
        关闭连接池。
        """
        self.__pool.close()


def demo():
    config = {
        'user': 'dazh',
        'password': 'dazh',
        'host': '192.168.101.215',
        'port': 1521,
        'sid': '',
        'service_name': 'dazh'
    }

    sql = "SELECT * FROM fcdb.TQ_SK_BASICINFO"

    orcl = OraclePool(config)
    result = orcl.fetch_all(sql)
    print(result)


if __name__ == "__main__":
    pass
    # current_path = os.path.abspath(".")
    # yaml_path = os.path.join(current_path, "config.yaml")
    # yaml_data = get_yaml_data(yaml_path)
    # print(yaml_data)
    # parse_zmq_pb_main()
    # get_topics()
    ts = get_consumer_topics_()
    print(ts)
    demo()
    # delete_topics(list(ts))
    # create_topic("quote-dev-1")quote-dev-2
    # print(msec_to_tdftime(33300000))
