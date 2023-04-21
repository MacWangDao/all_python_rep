import json
import sys
import os

current_path = os.path.dirname(os.path.abspath(__file__))
father_path = os.path.dirname(current_path)
father_path = os.path.dirname(father_path)
sys.path.append(father_path)
from concurrent.futures import ProcessPoolExecutor
import yaml
from loguru import logger

# from lob import LimitOrderBook, is_hour_between
# from lob import snapshot_generator_classic
# from lob import snapshot_time_checker
# from lob import sp_sl_to_influx
# from lob import int_ex_to_str

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
logger.add("lob9.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")

bootstrap_servers = None
token = None
org = None
bucket = None
url = None
time_interval = 3
topics = None
date_time = None
exchange = None
kafka_all_data_flag = False
orcl_config = {}


@logger.catch
def get_yaml_data(yaml_file):
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    data = yaml.load(file_data, Loader=yaml.Loader)
    return data


@logger.catch
def load_configuration():
    global bootstrap_servers
    global token
    global org
    global bucket
    global url
    global time_interval
    global exchange
    global topics
    global date_time
    global kafka_all_data_flag
    global orcl_config

    current_path = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(current_path, "config.yaml")
    yaml_data = get_yaml_data(yaml_path)
    topics = yaml_data.get("kafka_server", {}).get("topics")
    bootstrap_servers = yaml_data.get("kafka_server", {}).get("bootstrap_servers")
    date_time = yaml_data.get("date_time")
    token = yaml_data.get("influxdb", {}).get("token")
    org = yaml_data.get("influxdb", {}).get("org")
    bucket = yaml_data.get("influxdb", {}).get("bucket")
    url = yaml_data.get("influxdb", {}).get("url")
    time_interval = yaml_data.get("time_interval")
    kafka_all_data_flag = yaml_data.get("seek_to_beginning")
    orcl_config = {
        'user': yaml_data.get("oracle", {}).get("user"),
        'password': yaml_data.get("oracle", {}).get("password"),
        'host': yaml_data.get("oracle", {}).get("host"),
        'port': yaml_data.get("oracle", {}).get("port"),
        'service_name': yaml_data.get("oracle", {}).get("service_name")
    }
    # lclose_price_lookup_tab = get_lclose_price(config)
    return topics


@logger.catch
def kafka_to_gen_snapshots(topic, consumer=None, instruments=None, date_time=None, influx_writer=None):
    date_str_list = []
    # consumer.subscribe([topic])
    timestamp = set_datetime_offsets(consumer, topic, date_time=date_time)
    # global lob_map
    lclose_price_lookup_tab = get_lclose_price(orcl_config)
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
            input_code = instruments
            if input_code:
                # 获取配置文件中的股票代码信息
                if inst_code in input_code:
                    pass
                else:
                    continue
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
                transaction_pb = protobuf_transactions_parse(msg.value)
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
def set_datetime_offsets(consumer, topic, date_time=None):
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
def init_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id="quote_lob_" + get_datetime_str(),
        enable_auto_commit=False,
        # auto_commit_interval_ms=5000,
        auto_offset_reset='earliest',
    )
    return consumer


@logger.catch
def multiproces_topics(topic, date_time=None):
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id="quote_lob_" + get_datetime_str(),
        enable_auto_commit=False,
        # auto_commit_interval_ms=5000,
        auto_offset_reset='earliest',
    )
    influx_client, influx_writer = init_influx_client(url=url, token=token, org=org)
    kafka_to_gen_snapshots(topic, date_time=date_time, influx_writer=influx_writer, consumer=consumer)


@logger.catch
def init_influx_client(url=None, token=None, org=None):
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=ASYNCHRONOUS)
    logger.info("influxdb client initialized.")
    return client, write_api


@logger.catch
def start_multiproces_topics_run():
    global topics
    topics = load_configuration()
    topics = list(set(topics))
    with ProcessPoolExecutor(max_workers=len(topics)) as executor:
        for topic in topics:
            kargs = {"date_time": date_time}
            executor.submit(multiproces_topics, (topic), **kargs)


if __name__ == "__main__":
    start_multiproces_topics_run()
