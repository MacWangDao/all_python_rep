import asyncio
import datetime
import sys
import os

from aiokafka import AIOKafkaConsumer

current_path = os.path.dirname(os.path.abspath(__file__))
father_path = os.path.dirname(current_path)
father_path = os.path.dirname(father_path)
sys.path.append(father_path)
import yaml
from loguru import logger
from lob_new import LimitOrderBook, is_hour_between
from lob_new import snapshot_generator_classic
from lob_new import snapshot_time_checker
from lob_new import sp_sl_to_influx
from lob_new import int_ex_to_str
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata, KafkaProducer
import pandas as pd

from pykutils import local_time_timestamp, DateEncoder, protobuf_transactions_parse, protobuf_order_parse, \
    parse_pb
from oracleutils import get_lclose_price

from dtutils import get_datetime_str

logger.remove(handler_id=None)
logger.add("lob3.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")

time_interval = 3


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
def format_tdftime_f(data_list):
    merged_datetime = data_list[0] + data_list[2].zfill(9)
    formated_datetime = merged_datetime[0:4] + '-' + \
                        merged_datetime[4:6] + '-' + \
                        merged_datetime[6:8] + '-' + \
                        merged_datetime[8:10] + '-' + \
                        merged_datetime[10:12] + '-' + \
                        merged_datetime[12:14] + '-' + \
                        merged_datetime[14:17]

    return pd.to_datetime(formated_datetime, format="%Y-%m-%d-%H-%M-%S-%f")


@logger.catch
def init_influx_client(url=None, token=None, org=None):
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=ASYNCHRONOUS)
    logger.info("influxdb client initialized.")
    return client, write_api


async def set_datetime_offset_consumer(topic, bootstrap_servers, date_time=None):
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id="quote_lob_" + get_datetime_str(), enable_auto_commit=False, auto_offset_reset='earliest')
    await consumer.start()
    available_partitions = consumer.partitions_for_topic(topic)
    if not available_partitions:
        raise Exception("topic not exist.")
    if not date_time:
        now = datetime.datetime.now()
        otherStyleTime = now.strftime("%Y-%m-%d")
        otherStyleTime = otherStyleTime + " 09:00:00"
    else:
        otherStyleTime = date_time
    timestamp = datetime.datetime.strptime(otherStyleTime, "%Y-%m-%d %H:%M:%S").timestamp()
    timestamp = int(timestamp) * 1000
    dtps = {}
    tps = [TopicPartition(topic, tp) for tp in range(len(available_partitions))]
    for tp in tps:
        dtps[tp] = timestamp
    offsets = await consumer.offsets_for_times(dtps)
    consumer.assign(tps)
    for tp in consumer.assignment():
        offsetAndTimestamp = offsets[tp]
        if offsetAndTimestamp is not None:
            consumer.seek(tp, offsetAndTimestamp.offset)
        else:
            await consumer.seek_to_end(tp)
    return consumer


def lob_send_infliuxdb(request, influx_writer, orcl_config, lclose_price_lookup_tab):
    date_str_list = []
    lob_map = {}
    snapshot_ts_map = {}
    code = None
    key = request.key.decode()
    # 获取消费者key值
    inst_code, ex_code = tuple(key.split("."))
    # 过滤 获取上海6开头和深圳3或者0开头的信息
    if (ex_code == "1" and inst_code.startswith('6')) \
            or (ex_code == "1" and inst_code.startswith('5')) \
            or ((ex_code == "2" and inst_code.startswith('3')) or (ex_code == "2" and inst_code.startswith('0'))):
        newdata_ts = None
        if request.headers:
            # 获取headers中的k,v区分order和transaction的数据
            k_info, code = request.headers[0]
            code = code.decode()
        if code == "321":
            # 处理kafka protoc中order数据
            order_pb = protobuf_order_parse(request.value)
            order_data_list = parse_pb(order_pb)
            ex = order_data_list[3]  # 市场
            strex = int_ex_to_str(ex)
            instrument = order_data_list[4]  # 股票代码
            strdate = order_data_list[0]  # 日期
            if strdate not in date_str_list:
                date_str_list.append(strdate)
                if len(date_str_list) >= 2:
                    lclose_price_lookup_tab = get_lclose_price(orcl_config)
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
            transaction_pb = protobuf_transactions_parse(request.value)
            transaction_data_list = parse_pb(transaction_pb)
            ex = transaction_data_list[3]
            strex = int_ex_to_str(ex)
            instrument = transaction_data_list[4]
            strdate = transaction_data_list[0]
            if strdate not in date_str_list:
                date_str_list.append(strdate)
                if len(date_str_list) >= 2:
                    lclose_price_lookup_tab = get_lclose_price(orcl_config)
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
                    if snapshot_ts_map.get(key) is not None:
                        pass
                        # snapshot_on_prices[snapshot_ts_map.get(key)] = sp
                        # snapshot_on_lob[snapshot_ts_map.get(key)] = sl
                        # sp_sl_to_influx(strex + '.' + instrument, sp, sl, influx_writer, bucket=bucket, org=org)


async def service(topic, lc, lclose_price_lookup_tab):
    consumer = await set_datetime_offset_consumer(topic, lc.bootstrap_servers, lc.date_time)
    influx_client, influx_writer = init_influx_client(url=lc.url, token=lc.token, org=lc.org)

    async def consumer_request_async():
        try:
            async for request in consumer:
                # timestamp = request.timestamp / 1000
                # print(request.topic, datetime.datetime.utcfromtimestamp(timestamp))
                lob_send_infliuxdb(request, influx_writer, lc.oracle_config, lclose_price_lookup_tab)
        finally:
            await consumer.stop()

    asyncio.ensure_future(consumer_request_async())


async def main():
    lc = Load_configuration("config.yaml")
    lclose_price_lookup_tab = get_lclose_price(lc.oracle_config)
    topics = lc.topics
    tasks = []
    for topic in topics:
        task = asyncio.ensure_future(service(topic, lc, lclose_price_lookup_tab))
        tasks.append(task)
    await asyncio.wait(tasks)


if __name__ == "__main__":
    asyncio.run(main())
