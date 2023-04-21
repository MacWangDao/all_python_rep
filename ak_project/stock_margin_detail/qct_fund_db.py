import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import paramiko
from loguru import logger
import os

from pandas import ExcelWriter
from sqlalchemy import Integer, String, Column, Float, DateTime, Numeric
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

logger.add("log/qct_fund.log", rotation="100MB", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")


@as_declarative(class_registry=class_registry)
class Base:
    id: t.Any
    __name__: str

    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


class QCTFund(Base):
    __tablename__ = "T_QCT_BJHY_FUND_DETAIL"
    __table_args__ = {'comment': '全财通基金'}
    QCTID = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    QCT_TYPE = Column(String(100), nullable=True, comment="类型")
    SECURITY_CODE = Column(String(10), nullable=True, comment="股票代码", index=True)
    STOCKCODE = Column(String(20), nullable=True, comment="股票代码", index=True)
    SECURITY_NAME_ABBR = Column(String(100), nullable=True, comment="股票简称", index=True)
    PROG_PROGRESS = Column(String(20), nullable=True, comment="方案进度")
    QUOTED_TIME = Column(String(30), nullable=True, comment="报价时间")
    QUOTED_DATE = Column(DateTime, nullable=True, comment="报价日期")
    MIN_PRICE = Column(Numeric(15, 6), nullable=True, comment="底价")
    SUBSCRIBE_PRICE = Column(Numeric(15, 6), nullable=True, comment="认购门槛(万元)")
    LATEST_PRICE = Column(Numeric(15, 6), nullable=True, comment="最新价(元/股)")
    BID_WINNING_PRICE = Column(Numeric(15, 6), nullable=True, comment="中标价")
    MARKET_PRICE = Column(Numeric(15, 6), nullable=True, comment="报价当天市价")
    REINSTATEMENT_MARKET_PRICE = Column(Numeric(15, 6), nullable=True, comment="复权市价")
    LATEST_FLOATING_PROFIT = Column(Float, nullable=True, comment="最新浮盈")
    DISCOUNT_RATE = Column(Float, nullable=True, comment="折价率")
    MAX_AMOUNT = Column(Numeric(15, 6), nullable=True, comment="金额上限(亿元)")
    MAX_VOL = Column(Integer, nullable=True, comment="股数上限(万股)")
    SUBSCRIBE_VOL_MIN = Column(String(20), nullable=True, comment="认购起点")
    RAISING_SCALE = Column(Numeric(15, 6), nullable=True, comment="募集规模(亿元)")
    LIFTING_DAY = Column(DateTime, nullable=True, comment="解禁日")
    LIFTING_DAY_CLOSE_PRICE = Column(Numeric(15, 6), nullable=True, comment="解禁日收盘价)")
    LIFTING_DAY_FLOATING_PROFIT = Column(Float, nullable=True, comment="解禁浮盈")
    INSTORY_TYPE = Column(String(200), nullable=True, comment="申万行业分类")
    PROVINCE = Column(String(20), nullable=True, comment="所属省份")
    CITY = Column(String(20), nullable=True, comment="所属城市")
    UNDERWRITER = Column(String(200), nullable=True, comment="承销商")
    BALANCE_MARGIN_TRADING = Column(Numeric(15, 6), nullable=True, comment="融资融券余额(万元)")
    PROJECT_DATE = Column(DateTime, nullable=True, comment="项目日期")
    REMARKS_1 = Column(String(10), nullable=True, comment="备用1")
    REMARKS_2 = Column(String(10), nullable=True, comment="备用2")
    REMARKS_3 = Column(String(10), nullable=True, comment="备用3")
    CTIME = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    UPTIME = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    def __repr__(self):
        return f"QCTID:{self.QCTID},SECURITY_CODE:{self.SECURITY_CODE},SECURITY_NAME_ABBR:{self.SECURITY_NAME_ABBR},PROJECT_DATE:{self.PROJECT_DATE}"

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


def sk_code(s):
    s = str(s)
    if len(s.split(".")) == 2:
        code, ex = s.split(".")
        return code


def func_del_rep_date_data(pqct_date, qct_type):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    logger.info(
        f"DELETE FROM  T_QCT_BJHY_FUND_DETAIL WHERE PROJECT_DATE=to_date({pqct_date}, 'yyyy/mm/dd')  AND QCT_TYPE='{qct_type}'")
    cursor.execute(
        f"DELETE FROM  T_QCT_BJHY_FUND_DETAIL WHERE PROJECT_DATE=to_date({pqct_date}, 'yyyy/mm/dd')  AND QCT_TYPE='{qct_type}'")

    conn.commit()
    cursor.close()
    logger.info(f"cursor.close()")
    conn.close()
    logger.info(f"conn.close()")


def save_df_data(df):
    print(df.dtypes)
    print(df.shape)
    df.to_sql("T_QCT_BJHY_FUND_DETAIL", engine, index=False, chunksize=500, if_exists='append')
    return df


def date(para):
    if type(para) == int:
        delta = pd.Timedelta(str(int(para)) + 'days')
        time = pd.to_datetime('1899-12-30') + delta
        time = time.strftime('%Y%m%d')
        return time
    else:
        return para


def read_parse_qct_fund():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('max_colwidth', 100)
    path = "excel/全财通基金-每周定增项目表-2023-03-17.xlsx"
    pqct_date = "20230317"
    data = pd.read_excel(path, sheet_name=None)
    for k, v in data.items():
        if k == "定增项目信息表":
            df = v.copy()
            df = df.iloc[1:, 1:]
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "PROG_PROGRESS", "QUOTED_TIME", "MIN_PRICE",
                          "SUBSCRIBE_PRICE", "LATEST_PRICE", "DISCOUNT_RATE", "MAX_AMOUNT", "MAX_VOL", "INSTORY_TYPE",
                          "PROVINCE", "CITY", "UNDERWRITER", "BALANCE_MARGIN_TRADING"]
            df["MIN_PRICE"] = df["MIN_PRICE"].str.strip()
            df["MIN_PRICE"].replace("-", np.nan, inplace=True)
            df["DISCOUNT_RATE"] = df["DISCOUNT_RATE"].str.strip()
            df["DISCOUNT_RATE"].replace("-", np.nan, inplace=True)
            df["QCT_TYPE"] = k
            df["PROJECT_DATE"] = datetime.datetime.strptime(pqct_date, "%Y%m%d")
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["SECURITY_CODE"] = df["SECURITY_CODE"].apply(sk_code)
            df["MIN_PRICE"] = df["MIN_PRICE"].astype(np.float64)
            df["SUBSCRIBE_PRICE"] = df["SUBSCRIBE_PRICE"].astype(np.float64)
            df["LATEST_PRICE"] = df["LATEST_PRICE"].astype(np.float64)
            df["DISCOUNT_RATE"] = df["DISCOUNT_RATE"].astype(np.float64)
            df["MAX_AMOUNT"] = df["MAX_AMOUNT"].astype(np.float64)
            df["MAX_VOL"] = df["MAX_VOL"].astype(np.float64)
            df["BALANCE_MARGIN_TRADING"] = df["BALANCE_MARGIN_TRADING"].astype(np.float64)
            df["INSTORY_TYPE"] = df["INSTORY_TYPE"].astype(str)
            df["QUOTED_TIME"] = df["QUOTED_TIME"].apply(date)
            df["QUOTED_TIME"] = df["QUOTED_TIME"].astype(str)
            func_del_rep_date_data(pqct_date, k)
            save_df_data(df)
        elif k == "发审会之前项目":
            df = v.copy()
            df = df.iloc[1:, 1:]
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QUOTED_TIME", "LATEST_PRICE",
                          "MAX_AMOUNT", "MAX_VOL", "UNDERWRITER", "INSTORY_TYPE", "PROVINCE", "CITY",
                          "BALANCE_MARGIN_TRADING"]
            df["QCT_TYPE"] = k
            df["PROJECT_DATE"] = datetime.datetime.strptime(pqct_date, "%Y%m%d")
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["SECURITY_CODE"] = df["SECURITY_CODE"].apply(sk_code)
            df["LATEST_PRICE"] = df["LATEST_PRICE"].astype(np.float64)
            df["MAX_AMOUNT"] = df["MAX_AMOUNT"].astype(np.float64)
            df["MAX_VOL"] = df["MAX_VOL"].astype(np.float64)
            df["BALANCE_MARGIN_TRADING"] = df["BALANCE_MARGIN_TRADING"].astype(np.float64)
            df["INSTORY_TYPE"] = df["INSTORY_TYPE"].astype(str)
            func_del_rep_date_data(pqct_date, k)
            save_df_data(df)

        elif k == "已结束项目":
            df = v.copy()
            df = df.iloc[1:, 1:]
            df.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "QUOTED_DATE", "MIN_PRICE", "BID_WINNING_PRICE",
                          "MARKET_PRICE", "DISCOUNT_RATE", "REINSTATEMENT_MARKET_PRICE", "LATEST_FLOATING_PROFIT",
                          "SUBSCRIBE_VOL_MIN", "RAISING_SCALE", "LIFTING_DAY",
                          "LIFTING_DAY_CLOSE_PRICE", "LIFTING_DAY_FLOATING_PROFIT", "UNDERWRITER"]
            df["PROJECT_DATE"] = datetime.datetime.strptime(pqct_date, "%Y%m%d")
            df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
            df["SECURITY_CODE"] = df["SECURITY_CODE"].apply(sk_code)
            df['QUOTED_DATE'] = pd.to_datetime(df['QUOTED_DATE'])
            df['LIFTING_DAY'] = pd.to_datetime(df['LIFTING_DAY'])
            df["MIN_PRICE"] = df["MIN_PRICE"].astype(str)
            df['MIN_PRICE'] = df['MIN_PRICE'].str.replace(",", ".")
            df["MIN_PRICE"] = df["MIN_PRICE"].astype(np.float64)
            df["BID_WINNING_PRICE"] = df["BID_WINNING_PRICE"].astype(np.float64)
            df["MARKET_PRICE"] = df["MARKET_PRICE"].astype(np.float64)
            df["DISCOUNT_RATE"] = df["DISCOUNT_RATE"].astype(np.float64)
            df["REINSTATEMENT_MARKET_PRICE"] = df["REINSTATEMENT_MARKET_PRICE"].astype(np.float64)
            df["LATEST_FLOATING_PROFIT"] = df["LATEST_FLOATING_PROFIT"].astype(np.float64)
            df["RAISING_SCALE"] = df["RAISING_SCALE"].astype(np.float64)
            df["LIFTING_DAY_CLOSE_PRICE"] = df["LIFTING_DAY_CLOSE_PRICE"].astype(np.float64)
            df["LIFTING_DAY_FLOATING_PROFIT"] = df["LIFTING_DAY_FLOATING_PROFIT"].astype(np.float64)
            df["SUBSCRIBE_VOL_MIN"] = df["SUBSCRIBE_VOL_MIN"].astype(str)
            df["QCT_TYPE"] = k
            func_del_rep_date_data(pqct_date, k)
            save_df_data(df)


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
        xlsm = open('./excel' + '/每周定增项券源信息 ' + fordate + '.xlsx', "wb")
        with ftp_client.open(remote_path + '/每周定增项券源信息 ' + fordate + '.xlsx', "w") as f:
            xlsm_byes = buffer.getvalue()
            xlsm.write(xlsm_byes)
            f.write(xlsm_byes)
            logger.info('每周定增项券源信息 ' + fordate + '.xlsx')
    except Exception as e:
        logger.error(e)


def get_daily_qct_restricted_release(remote_path: str = "/home/guest/003-数据/008-解禁日可用券源信息",
                                     find_date: str = datetime.datetime.now().strftime("%Y%m%d")):
    sql = f"""
        SELECT DISTINCT TO_CHAR(t1.QY_DATE, 'YYYY-MM-DD') "可用券单日期",
       t2.QUOTED_TIME "报价时间",
       t2.PROG_PROGRESS "方案进度",
       t1.DATA_SOURCE "券源券商",
       t1.STOCKCODE "股票代码",
       t1.SECURITY_NAME_ABBR "股票名称",
       t1.QY_TYPE "券单从属",
       t1.AVAILABILITY_PERIOD "可用周期长度",
       t1.VOL "可用量",
       t1.ESTIMATED_SIZE "估计可用量",
       t2.SUBSCRIBE_PRICE "认购门槛(万元)",
       t2.MAX_AMOUNT "金额上限(亿元)",
       t2.MAX_VOL "股数上限(万股)",
       t2.INSTORY_TYPE "申万行业分类",
       t2.PROVINCE "所属省份",
       t2.CITY "所属城市",
       t2.UNDERWRITER "承销商",
       t2.BALANCE_MARGIN_TRADING "融资融券余额(万元)"
FROM T_QY_BJHY_STOCK_MARGIN_DETAIL t1
         JOIN (
    SELECT t.QCT_TYPE,
           t.STOCKCODE,
           t.PROG_PROGRESS,
           t.QUOTED_TIME,
           t.SUBSCRIBE_PRICE,
           t.MAX_AMOUNT,
           t.MAX_VOL,
           t.INSTORY_TYPE,
           t.PROVINCE,
           t.CITY,
           t.UNDERWRITER,
           t.BALANCE_MARGIN_TRADING
    FROM T_QCT_BJHY_FUND_DETAIL t
    WHERE t.QCT_TYPE = '定增项目信息表' AND t.QUOTED_TIME = (
        SELECT CASE
                   WHEN trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') <= TO_DATE({find_date}, 'yyyymmdd')
                       AND TO_DATE({find_date}, 'yyyymmdd') < trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') + 10
                       THEN '预计' || to_char(TO_DATE({find_date}, 'yyyymmdd'), 'fmmm') || '月上旬'
                   WHEN trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') + 10 <= TO_DATE({find_date}, 'yyyymmdd')
                       AND TO_DATE({find_date}, 'yyyymmdd') < trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') + 20
                       THEN '预计' || to_char(TO_DATE({find_date}, 'yyyymmdd'), 'fmmm') || '月中旬'
                   WHEN TO_DATE({find_date}, 'yyyymmdd') > trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') + 20
                       THEN '预计' || to_char(TO_DATE({find_date}, 'yyyymmdd'), 'fmmm') || '月下旬'
                   ELSE NULL
                   END AS QUOTED_TIME
        FROM dual
    )
       OR t.QUOTED_TIME = (
        SELECT '预计' || decode(to_char(TO_DATE({find_date}, 'yyyymmdd'), 'Q'), '1', '一', '2', '二', '3', '三', '4', '四') ||
               '季度' QUOTED_TIME
        from dual
    )
       OR t.QUOTED_TIME = (
        SELECT '预计' || to_char(TO_DATE({find_date}, 'yyyymmdd'), 'fmmm') || '月份' q from dual
    )
       OR t.QUOTED_TIME = (
        SELECT CASE
                   WHEN trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') <= TO_DATE({find_date}, 'yyyymmdd')
                       AND TO_DATE({find_date}, 'yyyymmdd') < trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') + 10
                       THEN '预计' || to_char(TO_DATE({find_date}, 'yyyymmdd'), 'fmmm') || '月初'
                   WHEN trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') + 10 <= TO_DATE({find_date}, 'yyyymmdd')
                       AND TO_DATE({find_date}, 'yyyymmdd') < trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') + 20
                       THEN '预计' || to_char(TO_DATE({find_date}, 'yyyymmdd'), 'fmmm') || '月中'
                   WHEN TO_DATE({find_date}, 'yyyymmdd') > trunc(TO_DATE({find_date}, 'yyyymmdd'), 'mm') + 20
                       THEN '预计' || to_char(TO_DATE({find_date}, 'yyyymmdd'), 'fmmm') || '月底'
                   ELSE NULL
                   END AS QUOTED_TIME
        FROM dual
    )
       OR t.QUOTED_TIME = '{find_date}'
       OR t.QUOTED_TIME = (
        SELECT '预计' || to_char(TO_DATE({find_date}, 'yyyymmdd'), 'fmmm"月"dd"日"') q from dual
    )
) t2 ON t1.STOCKCODE = t2.STOCKCODE AND t1.QY_DATE = TO_DATE({find_date}, 'yyyymmdd')
    """
    df_list = pd.read_sql(text(sql), engine.connect(), chunksize=5000)
    dflist = []
    for chunk in df_list:
        dflist.append(chunk)
    df = pd.concat(dflist)
    df.drop_duplicates(inplace=True)
    buffer = BytesIO()
    sheet_name = find_date
    with ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer,
                    sheet_name=sheet_name,
                    merge_cells=False, index=False)
    sftp_upload_xlsx(buffer, remote_path, find_date)


if __name__ == "__main__":
    # database_init()
    # read_parse_qct_fund()
    get_daily_qct_restricted_release()
