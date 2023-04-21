import asyncio
import datetime
import functools
import json
import random
import re
import time

import aiohttp
import numpy as np
import pandas as pd
import paramiko
from io import BytesIO
from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger
import pytz
import os

from pandas import ExcelWriter
from sqlalchemy import Integer, String, Column, Float, DateTime
from sqlalchemy.schema import Identity
from sqlalchemy.sql import func
import typing as t
import cx_Oracle
from sqlalchemy import create_engine, text

from sqlalchemy.ext.declarative import as_declarative, declared_attr
import warnings

warnings.filterwarnings("ignore")

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='dazh')
engine = create_engine("oracle://fcdb:fcdb@" + dns, echo=True)
class_registry: t.Dict = {}

logger.add("log/margin.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")


@as_declarative(class_registry=class_registry)
class Base:
    id: t.Any
    __name__: str

    # Generate __tablename__ automatically
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


# print(pytz.country_timezones('cn'))
shanghai = pytz.timezone('Asia/Shanghai')
scheduler = BlockingScheduler(timezone=shanghai)


def stock_code(s):
    s = str(s)
    cx = ""
    if len(s.split(".")) == 2:
        code, ex = s.split(".")
        cx = ex + "." + code
    else:
        if s.startswith("00"):
            cx = "SZ" + "." + s
        elif s.startswith("30"):
            cx = "SZ" + "." + s
        elif s.startswith("15"):
            cx = "SZ" + "." + s
        elif s.startswith("16"):
            cx = "SZ" + "." + s
        elif s.startswith("18"):
            cx = "SZ" + "." + s
        elif s.startswith("6"):
            cx = "SH" + "." + s
        elif s.startswith("50"):
            cx = "SH" + "." + s
        elif s.startswith("51"):
            cx = "SH" + "." + s
        elif s.startswith("52"):
            cx = "SH" + "." + s
        elif s.startswith("4"):
            cx = "BJ" + "." + s
        elif s.startswith("8"):
            cx = "BJ" + "." + s

    return cx


def func_del_rep_date_data(qy_date, data_source, qy_type):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    logger.info(
        f"DELETE FROM  T_QY_BJHY_STOCK_MARGIN_DETAIL WHERE QY_DATE=to_date({qy_date}, 'yyyy/mm/dd') AND DATA_SOURCE='{data_source}' AND QY_TYPE='{qy_type}'")
    cursor.execute(
        f"DELETE FROM  T_QY_BJHY_STOCK_MARGIN_DETAIL WHERE QY_DATE=to_date({qy_date}, 'yyyy/mm/dd') AND DATA_SOURCE='{data_source}' AND QY_TYPE='{qy_type}'")

    conn.commit()
    cursor.close()
    logger.info(f"cursor.close()")
    conn.close()
    logger.info(f"conn.close()")


def save_df_data(df):
    print(df.dtypes)
    print(df.shape)
    df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL", engine, index=False, chunksize=500, if_exists='append')
    # df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL_TMP", engine, index=False, chunksize=500, if_exists='append')
    return df


def sftp_upload_xlsx(buffer, remote_path, fordate):
    """
    上传excel文件，流方式
    """
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname='192.168.101.211', username='toptrade', password='toptrade')
        stdin, stdout, stderr = ssh_client.exec_command(
            "echo 'toptrade' | sudo -S chmod 777 /home/guest/003-数据/008-解禁日可用券源信息 -R", timeout=300)
        out = stdout.readlines()
        ftp_client = ssh_client.open_sftp()
        xlsm = open('./excel' + '/解禁日可用券源信息 ' + fordate + '.xlsx', "wb")
        with ftp_client.open(remote_path + '/解禁日可用券源信息 ' + fordate + '.xlsx', "w") as f:
            xlsm_byes = buffer.getvalue()
            xlsm.write(xlsm_byes)
            f.write(xlsm_byes)
            logger.info('解禁日可用券源信息 ' + fordate + '.xlsx')
    except Exception as e:
        logger.error(e)


class Resticted(Base):
    __tablename__ = "T_QY_BJHY_STOCK_MARGIN_DETAIL"
    __table_args__ = {'comment': '券源明细表'}
    QYID = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    DATA_SOURCE = Column(String(20), nullable=True, comment="中信建投 广发 银河 中信")
    SECURITY_CODE = Column(String(10), nullable=True, comment="股票代码", index=True)
    STOCKCODE = Column(String(20), nullable=True, comment="股票代码", index=True)
    SECURITY_NAME_ABBR = Column(String(100), nullable=True, comment="股票简称", index=True)
    QY_DATE = Column(DateTime, nullable=True, comment="券源日期", index=True)
    QY_TYPE = Column(String(100), nullable=True, comment="类型")
    COUPON_RATES = Column(Float, nullable=True, comment="券息")
    MIN_COUPON_RATES = Column(Float, nullable=True, comment="最低券息")
    ACCEPTABLE_UPPER_LIMIT = Column(Float, nullable=True, comment="可接受券息上限")
    AVAILABILITY_PERIOD = Column(String(100), nullable=True, comment="可提供期限")
    MIN_TERM = Column(Float, nullable=True, comment="最小期限")
    MAX_TERM = Column(Float, nullable=True, comment="最大期限")
    VOL = Column(Integer, nullable=True, comment="数量")
    BORROWABLE_VOL = Column(String(100), nullable=True, comment="可借数量")
    REQUIRED_VOL = Column(Integer, nullable=True, comment="所需数量")
    APPLY_VOL = Column(Integer, nullable=True, comment="申请数量")
    EXPECT_VOL = Column(Integer, nullable=True, comment="期望数量")
    MIN_VOL = Column(Integer, nullable=True, comment="最低数量")
    MAX_VOL = Column(Integer, nullable=True, comment="最低数量")
    POTENTIAL_VOL = Column(Integer, nullable=True, comment="潜在可借出数量")
    EXPIRATION_DATE = Column(DateTime, nullable=True, comment="到期日期")
    ESTIMATED_SIZE = Column(String(100), nullable=True, comment="预计规模")
    REMARKS_GF = Column(String(100), nullable=True, comment="备注")
    DAYS_AVB = Column(Integer, nullable=True, comment="可用天数")
    DAYS_REMAINING = Column(Integer, nullable=True, comment="剩余天数")
    REFERENCE_RATE = Column(Float, nullable=True, comment="参考费率")
    COST_PRICE = Column(Float, nullable=True, comment="成本价")
    INSTORY = Column(String(200), nullable=True, comment="行业")
    INDEX_NAME = Column(String(20), nullable=True, comment="所属指数")
    PROBABILITY = Column(String(20), nullable=True, comment="概率高低")
    EXPECT_PROB = Column(String(20), nullable=True, comment="期望概率")
    MARKET_VALUE = Column(Float, nullable=True, comment="预计市值(千万元)")
    MARKET_VALUE_DESC = Column(String(20), nullable=True, comment="市值描述")
    ADD_FLAG = Column(String(10), nullable=True, comment="新增标志")
    TRADE_MARKET = Column(String(10), nullable=True, comment="交易市场")
    AVAILABLE_TIME = Column(String(20), nullable=True, comment="可用券时间")
    LOCK_CUT_TIME = Column(String(16), nullable=True, comment="锁券申请截止时间")
    REMARKS_1 = Column(String(10), nullable=True, comment="备用1")
    REMARKS_2 = Column(String(10), nullable=True, comment="备用2")
    REMARKS_3 = Column(String(10), nullable=True, comment="备用3")
    CTIME = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    UPTIME = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    def __repr__(self):
        return f"QYID:{self.QYID},SECURITY_CODE:{self.SECURITY_CODE},SECURITY_NAME_ABBR:{self.SECURITY_NAME_ABBR},QY_DATE:{self.QY_DATE}," \
               f"QY_TYPE:{self.QY_TYPE},ctime:{self.ctime}"

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


def database_init():
    import cx_Oracle
    from sqlalchemy import create_engine
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='dazh')
    engine = create_engine("oracle://fcdb:fcdb@" + dns, encoding='utf-8', echo=True)
    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def filter_path(file):
    pass


def getHingtPath(path):
    find_date: str = datetime.datetime.now().strftime("%Y-%m-%d")
    flist = list()
    for root, dirs, files in os.walk(path):
        for file in files:
            ufile = re.split(f"{find_date}.xlsx", file)
            if len(ufile) == 2:
                flist.append(os.path.join(root, file))
    return flist


def parse_excel_file_zxjt_t0(path, find_date=None):
    path = f"/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/中信建投/T+0券源模板 {find_date}.xlsx"
    df = pd.read_excel(path, sheet_name=0, dtype={0: str})
    df = df.iloc[:, 1:10]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QY_TYPE", "BORROWABLE_VOL", "COUPON_RATES",
                  "AVAILABILITY_PERIOD", "MIN_TERM", "MAX_TERM", "REQUIRED_VOL"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["SOURCE"] = 1
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    print(df.dtypes)
    print(df)
    df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL", engine, index=False, chunksize=500, if_exists='append')


def parse_excel_file_zxjt_t1(path, find_date=None):
    path = f"/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/中信建投/T+1券源模板 {find_date}.xlsx"
    df = pd.read_excel(path, sheet_name=0, dtype={1: str})
    df = df.iloc[:, 1:11]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QY_TYPE", "MIN_COUPON_RATES", "AVAILABILITY_PERIOD",
                  "ACCEPTABLE_UPPER_LIMIT", "MIN_TERM", "MAX_TERM", "EXPECT_VOL", "MIN_VOL"]
    df["SECURITY_CODE"] = df["SECURITY_CODE"].astype(str)
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["SOURCE"] = 1
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    df["MIN_COUPON_RATES"].replace('请申报', -1, inplace=True)
    df["MIN_COUPON_RATES"] = df["MIN_COUPON_RATES"].astype(np.float64)
    df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL", engine, index=False, chunksize=500, if_exists='append')


def parse_excel_file_zxjt_sp(path, find_date=None):
    path = f"/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/中信建投/库存券源(审批)模板 {find_date}.xlsx"
    df = pd.read_excel(path, sheet_name=0, dtype={1: str})
    df = df.iloc[:, 1:8]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QY_TYPE", "VOL", "COUPON_RATES",
                  "EXPIRATION_DATE", "APPLY_VOL"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["SOURCE"] = 1
    df['EXPIRATION_DATE'] = pd.to_datetime(df['EXPIRATION_DATE'])
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    print(df.dtypes)
    print(df)
    df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL", engine, index=False, chunksize=500, if_exists='append')


def parse_excel_file_zxjt_mb(path, find_date=None):
    path = f"/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/中信建投/库存券源模板 {find_date}.xlsx"
    df = pd.read_excel(path, sheet_name=0, dtype={1: str})
    df = df.iloc[:, 1:8]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QY_TYPE", "VOL", "COUPON_RATES",
                  "EXPIRATION_DATE", "APPLY_VOL"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["SOURCE"] = 1
    df['EXPIRATION_DATE'] = pd.to_datetime(df['EXPIRATION_DATE'])
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    print(df.dtypes)
    print(df)
    df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL", engine, index=False, chunksize=500, if_exists='append')


def parse_excel_file_gf(path, find_date=None):
    path = f"/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/广发/专项券源名单 {find_date}.xlsx"
    data = pd.read_excel(path, sheet_name=None, dtype={0: str, 1: str})
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    for k, v in data.items():
        if k == "非公募券单":
            print(k)
            df = v.iloc[:, 1:6]
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "ESTIMATED_SIZE",
                          "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["SOURCE"] = 2
            df["QY_TYPE"] = k
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            df["ESTIMATED_SIZE"] = df["ESTIMATED_SIZE"].astype(str)

            df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL", engine, index=False, chunksize=500, if_exists='append')
        elif k == "公募券单":
            print(k)
            df = v.iloc[:, 1:5]
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "ESTIMATED_SIZE"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["SOURCE"] = 2
            df["QY_TYPE"] = k
            df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL", engine, index=False, chunksize=500, if_exists='append')
        elif k == "库存券":
            print(k)
            df = v.iloc[:, 0:6]
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "DAYS_REMAINING", "VOL", "REFERENCE_RATE",
                          "EXPIRATION_DATE"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["SOURCE"] = 2
            df["QY_TYPE"] = k
            df['EXPIRATION_DATE'] = pd.to_datetime(df['EXPIRATION_DATE'])
            df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL", engine, index=False, chunksize=500, if_exists='append')


def parse_excel_file_yh(path, find_date=None):
    path = f"/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/银河/实时锁券券单 {find_date}.xlsx"
    df = pd.read_excel(path, sheet_name=0, dtype={0: str, 1: str})
    # print(data)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df = df.iloc[:, 0:7]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "REFERENCE_RATE",
                  "VOL", "INSTORY"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    df["QY_DATE"] = find_date
    df["SOURCE"] = 3
    df["QY_TYPE"] = "实时锁券券单"
    df["REFERENCE_RATE"] = df["REFERENCE_RATE"].str.strip('%').astype(float) / 100

    # pd.set_option('display.max_columns', None)
    # 显示所有行
    # pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    # pd.set_option('max_colwidth', 100)
    df.to_sql("T_QY_BJHY_STOCK_MARGIN_DETAIL", engine, index=False, chunksize=500, if_exists='append')


def parse_excel_file_zxjt_t0_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                                  source: str = None):
    df = pd.read_excel(buffer, sheet_name=0, dtype={0: str})
    df = df.iloc[:, 1:10]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QY_TYPE", "BORROWABLE_VOL", "COUPON_RATES",
                  "AVAILABILITY_PERIOD", "MIN_TERM", "MAX_TERM", "REQUIRED_VOL"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["DATA_SOURCE"] = source
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].str.extract("(\d+/.*\d|\d+)", expand=False)
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    qy_date = find_date.strftime("%Y%m%d")
    data_source = source
    qy_type = "T+0券源"
    func_del_rep_date_data(qy_date, data_source, qy_type)
    save_df_data(df)


def parse_excel_file_zxjt_t1_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                                  source: str = None):
    df = pd.read_excel(buffer, sheet_name=0, dtype={1: str})
    df = df.iloc[:, 1:11]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QY_TYPE", "MIN_COUPON_RATES", "AVAILABILITY_PERIOD",
                  "ACCEPTABLE_UPPER_LIMIT", "MIN_TERM", "MAX_TERM", "EXPECT_VOL", "MIN_VOL"]
    df["SECURITY_CODE"] = df["SECURITY_CODE"].astype(str)
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["DATA_SOURCE"] = source
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    df["MIN_COUPON_RATES"].replace('请申报', -1, inplace=True)
    df["MIN_COUPON_RATES"] = df["MIN_COUPON_RATES"].astype(np.float64)
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].str.extract("(\d+/.*\d|\d+)", expand=False)
    qy_date = find_date.strftime("%Y%m%d")
    data_source = source
    qy_type = "T+1券源"
    func_del_rep_date_data(qy_date, data_source, qy_type)
    save_df_data(df)


def parse_excel_file_zxjt_sp_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                                  source: str = None):
    df = pd.read_excel(buffer, sheet_name=0, dtype={1: str})
    df = df.iloc[:, 1:8]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QY_TYPE", "VOL", "COUPON_RATES",
                  "EXPIRATION_DATE", "APPLY_VOL"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["DATA_SOURCE"] = source
    df['EXPIRATION_DATE'] = pd.to_datetime(df['EXPIRATION_DATE'])
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    qy_date = find_date.strftime("%Y%m%d")
    data_source = source
    qy_type = "库存券源(审批)"
    func_del_rep_date_data(qy_date, data_source, qy_type)
    save_df_data(df)


def parse_excel_file_zxjt_mb_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                                  source: str = None):
    df = pd.read_excel(buffer, sheet_name=0, dtype={1: str})
    df = df.iloc[:, 1:8]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QY_TYPE", "VOL", "COUPON_RATES",
                  "EXPIRATION_DATE", "APPLY_VOL"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["DATA_SOURCE"] = source
    df['EXPIRATION_DATE'] = pd.to_datetime(df['EXPIRATION_DATE'])
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    qy_date = find_date.strftime("%Y%m%d")
    data_source = source
    qy_type = "库存券源"
    func_del_rep_date_data(qy_date, data_source, qy_type)
    save_df_data(df)


def parse_excel_file_yh_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                             source: str = None):
    data = pd.read_excel(buffer, sheet_name=None, dtype={0: str, 1: str})
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    for k, v in data.items():
        if k == "非公募券单":
            print(k)
            df = v.iloc[:, 1:6]
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "ESTIMATED_SIZE",
                          "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            df["ESTIMATED_SIZE"] = df["ESTIMATED_SIZE"].astype(str)
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].str.extract("(\d+/.*\d|\d+)", expand=False)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "公募券单":
            print(k)
            df = v.iloc[:, 1:5]
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "ESTIMATED_SIZE"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].str.extract("(\d+/.*\d|\d+)", expand=False)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "库存券":
            print(k)
            df = v.iloc[:, 0:6]
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "DAYS_REMAINING", "VOL", "REFERENCE_RATE",
                          "EXPIRATION_DATE"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            df['EXPIRATION_DATE'] = pd.to_datetime(df['EXPIRATION_DATE'])
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)


def parse_excel_file_gf_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                             source: str = None):
    df = pd.read_excel(buffer, sheet_name=0, dtype={0: str, 1: str})
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df = df.iloc[:, 0:7]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "REFERENCE_RATE",
                  "VOL", "INSTORY"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    df["QY_DATE"] = find_date
    df["DATA_SOURCE"] = source
    df["QY_TYPE"] = "实时锁券券单"
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].str.extract("(\d+/.*\d|\d+)", expand=False)
    df["REFERENCE_RATE"] = df["REFERENCE_RATE"].str.strip('%').astype(float) / 100

    # pd.set_option('display.max_columns', None)
    # 显示所有行
    # pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    # pd.set_option('max_colwidth', 100)
    qy_date = find_date.strftime("%Y%m%d")
    data_source = source
    qy_type = "实时锁券券单"
    func_del_rep_date_data(qy_date, data_source, qy_type)
    save_df_data(df)


def parse_excel_file_zx_orgin(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                              source: str = None):
    # data = pd.read_excel(buffer, sheet_name=None, dtype={0: str, 1: str})
    # find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    path = "/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/中信/中信券单列表20230201091117.xlsx"
    (filepath, tempfilename) = os.path.split(path)
    (filename, extension) = os.path.splitext(tempfilename)
    find_date_s = filename.split("中信券单列表")[1][:8]
    find_date = datetime.datetime.strptime(find_date_s, "%Y%m%d")
    data = pd.read_excel(path, sheet_name=None, dtype={0: str})
    source = "中信"

    for k, v in data.items():
        if k == "经纪券单(T+1)":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["证券代码", "证券名称", "7", "14", "28", "91", "182", "备注"]
            test_data = pd.melt(frame=df  # 待转换df
                                , id_vars=['证券代码', '证券名称']  # 固定的列
                                , value_vars=['7', '14', '28', '91', '182']  # 待转换的列名
                                , var_name='期限'  # 行转列转换后的列名称
                                , value_name='数量'  # 最后数据列名称
                                )
            df = df.iloc[:, [0, -1]]
            df = pd.merge(test_data, df, how='inner', on="证券代码")
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "VOL",
                          "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "经纪券单（预约T+0）":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["证券代码", "证券名称", "7", "14", "28", "91", "182", "备注"]
            test_data = pd.melt(frame=df  # 待转换df
                                , id_vars=['证券代码', '证券名称']  # 固定的列
                                , value_vars=['7', '14', '28', '91', '182']  # 待转换的列名
                                , var_name='期限'  # 行转列转换后的列名称
                                , value_name='数量'  # 最后数据列名称
                                )
            df = df.iloc[:, [0, -1]]
            df = pd.merge(test_data, df, how='inner', on="证券代码")
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "VOL",
                          "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "公募券单":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "MAX_VOL", "AVAILABILITY_PERIOD", "INDEX_NAME",
                          "PROBABILITY", "EXPECT_PROB", "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "篮子下单(T+1,按权重)":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "POTENTIAL_VOL", "AVAILABILITY_PERIOD", "INDEX_NAME",
                          "PROBABILITY", "EXPECT_PROB", "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "实时T+0券单":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "VOL", "AVAILABILITY_PERIOD"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "长期券源":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "MARKET_VALUE", "AVAILABILITY_PERIOD", "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)


def parse_excel_file_gf_quggxx_orgin(buffer: bytes = None,
                                     find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                                     source: str = None):
    # data = pd.read_excel(buffer, sheet_name=None, dtype={0: str, 1: str})
    # find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    path = "/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/广发/券源供给信息 2023-02-08.xlsx"
    (filepath, tempfilename) = os.path.split(path)
    (filename, extension) = os.path.splitext(tempfilename)
    find_date_s = filename.split(" ")[1].split(".")[0]
    find_date = datetime.datetime.strptime(find_date_s, "%Y-%m-%d")
    data = pd.read_excel(path, sheet_name=0, dtype={0: str, 1: str, 2: str, 4: int})
    source = "广发"


def parse_excel_file_zx_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                             source: str = None):
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    data = pd.read_excel(buffer, sheet_name=None, dtype={0: str})
    for k, v in data.items():
        if k == "经纪券单(T+1)":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["证券代码", "证券名称", "7", "14", "28", "91", "182", "备注"]
            test_data = pd.melt(frame=df  # 待转换df
                                , id_vars=['证券代码', '证券名称']  # 固定的列
                                , value_vars=['7', '14', '28', '91', '182']  # 待转换的列名
                                , var_name='期限'  # 行转列转换后的列名称
                                , value_name='数量'  # 最后数据列名称
                                )
            df = df.iloc[:, [0, -1]]
            df = pd.merge(test_data, df, how='inner', on="证券代码")
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "VOL",
                          "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "经纪券单（预约T+0）":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["证券代码", "证券名称", "7", "14", "28", "91", "182", "备注"]
            test_data = pd.melt(frame=df  # 待转换df
                                , id_vars=['证券代码', '证券名称']  # 固定的列
                                , value_vars=['7', '14', '28', '91', '182']  # 待转换的列名
                                , var_name='期限'  # 行转列转换后的列名称
                                , value_name='数量'  # 最后数据列名称
                                )
            df = df.iloc[:, [0, -1]]
            df = pd.merge(test_data, df, how='inner', on="证券代码")
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "VOL",
                          "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "公募券单":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "MAX_VOL", "AVAILABILITY_PERIOD", "INDEX_NAME",
                          "PROBABILITY", "EXPECT_PROB", "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "篮子下单(T+1,按权重)":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "POTENTIAL_VOL", "AVAILABILITY_PERIOD", "INDEX_NAME",
                          "PROBABILITY", "EXPECT_PROB", "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "实时T+0券单":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "VOL", "AVAILABILITY_PERIOD"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)
        elif k == "长期券源":
            print(k)
            df = v.copy()
            df['证券代码'] = df['证券代码'].str.zfill(6)
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "MARKET_VALUE", "AVAILABILITY_PERIOD", "REMARKS_GF"]
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["QY_DATE"] = find_date
            df["DATA_SOURCE"] = source
            df["QY_TYPE"] = k
            # df['QY_DATE'] = pd.to_datetime(df['QY_DATE'])
            df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth', 100)
            qy_date = find_date.strftime("%Y%m%d")
            data_source = source
            qy_type = k
            func_del_rep_date_data(qy_date, data_source, qy_type)
            save_df_data(df)


def parse_excel_file_gf_qyggxx_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                                    source: str = None):
    qy_type = "券源供给信息"
    df = pd.read_excel(buffer, sheet_name=0, dtype={0: str, 2: str, 3: str, 4: int})
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "REFERENCE_RATE", "VOL", "ADD_FLAG",
                  "INDEX_NAME"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["DATA_SOURCE"] = source
    df["QY_TYPE"] = qy_type
    df["REFERENCE_RATE"] = df["REFERENCE_RATE"].str.strip('%').astype(float) / 100
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].str.extract("(\d+/.*\d|\d+)", expand=False)
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    qy_date = find_date.strftime("%Y%m%d")
    data_source = source
    func_del_rep_date_data(qy_date, data_source, qy_type)
    save_df_data(df)


def parse_excel_file_yh_tlqd_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                                  source: str = None):
    qy_type = "套利券单"
    df = pd.read_excel(buffer, sheet_name=0, dtype={0: str})
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "MARKET_VALUE_DESC", "AVAILABILITY_PERIOD"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["DATA_SOURCE"] = source
    df["QY_TYPE"] = qy_type
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].str.extract("(\d+/.*\d|\d+)", expand=False)
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    qy_date = find_date.strftime("%Y%m%d")
    data_source = source
    func_del_rep_date_data(qy_date, data_source, qy_type)
    save_df_data(df)


def parse_excel_file_zj_t0_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                                source: str = None):
    qy_type = "T0券单"
    df = pd.read_excel(buffer, sheet_name=0, dtype=str, skiprows=19)
    df = df.iloc[:, 1:]
    df.columns = ["EXPIRATION_DATE", "TRADE_MARKET", "SECURITY_CODE", "SECURITY_NAME_ABBR", "DAYS_AVB",
                  "COST_PRICE", "EXPECT_VOL", "VOL", "REFERENCE_RATE"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    df["REFERENCE_RATE"] = df["REFERENCE_RATE"].astype(float) / 100
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["DATA_SOURCE"] = source
    df["QY_TYPE"] = qy_type
    df["EXPIRATION_DATE"] = pd.to_datetime(df["EXPIRATION_DATE"])
    df["DAYS_AVB"] = df["DAYS_AVB"].astype(int)
    df["COST_PRICE"] = df["COST_PRICE"].astype(float)
    df["EXPECT_VOL"] = df["EXPECT_VOL"].astype(int)
    df["VOL"] = df["VOL"].astype(int)
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    qy_date = find_date.strftime("%Y%m%d")
    data_source = source
    func_del_rep_date_data(qy_date, data_source, qy_type)
    save_df_data(df)


def parse_excel_file_zj_rjq_sftp(buffer: bytes = None, find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                                 source: str = None):
    qy_type = "日间券单"
    df = pd.read_excel(buffer, sheet_name=0, dtype={1: str, 3: str, 7: str})
    df = df.iloc[:, 1:]
    df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "AVAILABILITY_PERIOD", "PROBABILITY", "MIN_TERM",
                  "AVAILABLE_TIME", "LOCK_CUT_TIME"]
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d")
    df["QY_DATE"] = find_date
    df["DATA_SOURCE"] = source
    df["QY_TYPE"] = qy_type
    # print(df["AVAILABILITY_PERIOD"])
    df["LOCK_CUT_TIME"] = df["LOCK_CUT_TIME"].astype(str)
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].astype(str)
    df["AVAILABILITY_PERIOD"] = df["AVAILABILITY_PERIOD"].str.extract("(\d+、.*\d|\d+)", expand=False)
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    qy_date = find_date.strftime("%Y%m%d")
    data_source = source
    func_del_rep_date_data(qy_date, data_source, qy_type)
    save_df_data(df)


def parse_excel_file_sftp(buffer: bytes = None, source: str = None, qytype: str = None,
                          find_date: str = datetime.datetime.now().strftime("%Y-%m-%d")):
    print(qytype)
    if source == "中信建投":
        if qytype == "T+0券源模板":
            parse_excel_file_zxjt_t0_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")
        elif qytype == "T+1券源模板":
            parse_excel_file_zxjt_t1_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")
        elif qytype == "库存券源(审批)模板":
            parse_excel_file_zxjt_sp_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")
        elif qytype == "库存券源模板":
            parse_excel_file_zxjt_mb_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")
    elif source == "广发":
        if qytype == "实时锁券券单":
            parse_excel_file_gf_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")
        elif qytype == "券源供给信息":
            parse_excel_file_gf_qyggxx_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")
    elif source == "银河":
        if qytype == "专项券源名单":
            parse_excel_file_yh_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")
        elif qytype == "套利券单":
            parse_excel_file_yh_tlqd_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")
    elif source == "中信":
        parse_excel_file_zx_sftp(buffer, find_date, source)
        logger.info(f"{qytype}完成!")
    elif source == "中金":
        if qytype == "T0券单":
            parse_excel_file_zj_t0_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")
        elif qytype == "日间券单":
            parse_excel_file_zj_rjq_sftp(buffer, find_date, source)
            logger.info(f"{qytype}完成!")


def sftp_download_excel(remote_path: str = "/home/guest/003-数据/007-每日券源",
                        find_date: str = datetime.datetime.now().strftime("%Y-%m-%d"),
                        dsource: str = None):
    host = '192.168.101.211'  # sftp主机
    port = 22  # 端口
    username = 'toptrade'  # sftp用户名
    password = 'toptrade'  # 密码
    sf = paramiko.Transport((host, port))
    sf.connect(username=username, password=password)
    try:
        sftp = paramiko.SFTPClient.from_transport(sf)
        dres = sftp.listdir(remote_path)
        for dir in dres:
            if dsource == dir or dsource is None:
                dpath = os.path.join(remote_path, dir)
                download_filenames = sftp.listdir(dpath)
                for download_filename in download_filenames:
                    # print(download_filename)
                    filename = re.split(f"{find_date}.xlsx", download_filename)
                    if len(filename) == 2:
                        excel_path_file = os.path.join(dpath, download_filename)
                        # print(download_filename)
                        qytypes = download_filename.split(" ")
                        if len(qytypes) > 0:
                            qytype = qytypes[0]
                            with sftp.open(excel_path_file) as remote_file:
                                logger.info(find_date)
                                logger.info(excel_path_file)
                                parse_excel_file_sftp(remote_file.read(), dir, qytype, find_date)
    except Exception as e:
        logger.error(e)
    finally:
        sf.close()


def get_daily_qy_restricted_release(remote_path: str = "/home/guest/003-数据/008-解禁日可用券源信息",
                                    find_date: str = datetime.datetime.now().strftime("%Y-%m-%d")):
    sql = f"""
        SELECT
            T1.STOCKCODE "市场.代码",
            T1.SECURITY_NAME_ABBR "股票名称",
            TO_CHAR(T1.FREE_DATE,'YYYY-MM-DD') "解禁日",
            T1.FREE_SHARES_TYPE "解禁类型",
            T1.FREE_RATIO "占解禁前流通市值比例",
            T1.LIFT_MARKET_CAP "实际解禁市值(万元)",
            TO_CHAR(T2.QY_DATE,'YYYY-MM-DD') "可用券单日期",
            T2.DATA_SOURCE "券源券商",
            T2.QY_TYPE "券单从属",
            T2.AVAILABILITY_PERIOD "可用周期长度",
            T2.VOL "可用量",
            T2.ESTIMATED_SIZE "估计可用量"
        FROM
            T_EM_BJHY_RESTRICTED_RELEASE T1,
            T_QY_BJHY_STOCK_MARGIN_DETAIL T2
        WHERE
            (T1.FREE_SHARES_TYPE LIKE '%定向增发机构配售股份%')
            AND T1.FREE_DATE >= TO_DATE('{find_date}', 'YYYY-MM-DD')
            AND T1.FREE_DATE < ADD_MONTHS(TO_DATE('{find_date}', 'YYYY-MM-DD'),1)
            AND T2.QY_DATE = TO_DATE('{find_date}', 'YYYY-MM-DD')
            AND T2.STOCKCODE = T1.STOCKCODE
        ORDER BY
            T1.FREE_DATE
    """
    df_list = pd.read_sql(text(sql), engine.connect(), chunksize=5000)
    dflist = []
    for chunk in df_list:
        dflist.append(chunk)
    df = pd.concat(dflist)
    buffer = BytesIO()
    sheet_name = find_date
    with ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer,
                    sheet_name=sheet_name,
                    merge_cells=False, index=False)
    sftp_upload_xlsx(buffer, remote_path, find_date)


def method_file_test():
    pass


if __name__ == "__main__":
    # path = "/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源"
    # find_date = "2023-02-09"
    # parse_excel_file(path)
    # parse_excel_file_zxjt_t0(path, find_date)
    # parse_excel_file_zxjt_t1(path, find_date)
    # parse_excel_file_zxjt_sp(path, find_date)
    # parse_excel_file_zxjt_mb(path, find_date)
    # parse_excel_file_gf(path, find_date)
    # parse_excel_file_yh(path, find_date)
    # database_init()
    # remote_path = "/home/guest/003-数据/007-每日券源"
    # sftp_download_excel(remote_path, find_date, dsource="银河")
    # sftp_download_excel(remote_path, dsource="银河")
    # sftp_download_excel(remote_path, find_date)
    # sftp_download_excel(remote_path)
    #########################################
    # find_date = "2023-02-10"
    # sftp_download_excel(remote_path, find_date)
    # sftp_download_excel(remote_path, find_date, dsource="中金")
    ###############################
    # sftp_download_excel(dsource="中信建投")
    # sftp_download_excel(dsource="银河")
    # sftp_download_excel(dsource="中信")
    sftp_download_excel()
    ####################################
    get_daily_qy_restricted_release()
