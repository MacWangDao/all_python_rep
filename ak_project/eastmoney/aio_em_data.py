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

from apscheduler.schedulers.blocking import BlockingScheduler
from dateutil.relativedelta import relativedelta
from loguru import logger
import pytz
import os
from sqlalchemy import Integer, String, Column, Float, DateTime
from sqlalchemy.schema import Identity
from sqlalchemy.sql import func
import typing as t
import cx_Oracle
from sqlalchemy import create_engine

from sqlalchemy.ext.declarative import as_declarative, declared_attr
import warnings

warnings.filterwarnings("ignore")

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='dazh')
engine = create_engine("oracle://fcdb:fcdb@" + dns, encoding='utf-8', echo=True)
class_registry: t.Dict = {}

logger.add("log/em.log", rotation="100MB", encoding="utf-8", enqueue=True,
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
    cx = ""
    if len(s.split(".")) == 2:
        code, ex = s.split(".")
        cx = ex + "." + code
    else:
        if s.startswith("00"):
            cx = "SZ" + "." + s
        elif s.startswith("30"):
            cx = "SZ" + "." + s
        elif s.startswith("6"):
            cx = "SH" + "." + s
        elif s.startswith("4"):
            cx = "BJ" + "." + s
        elif s.startswith("8"):
            cx = "BJ" + "." + s

    return cx


def save_df_data(df):
    df["STOCKCODE"] = df["SECURITY_CODE"].apply(stock_code)
    df['FREE_DATE'] = pd.to_datetime(df['FREE_DATE'])
    df['BATCH_HOLDER_NUM'].fillna(0, inplace=True)
    df['B20_ADJCHRATE'].fillna(0, inplace=True)
    df['A20_ADJCHRATE'].fillna(0, inplace=True)
    df['BATCH_HOLDER_NUM'] = df['BATCH_HOLDER_NUM'].astype(np.int64)
    df['CURRENT_FREE_SHARES'] = df['CURRENT_FREE_SHARES'].astype(np.float64)
    df['ABLE_FREE_SHARES'] = df['ABLE_FREE_SHARES'].astype(np.float64)
    df['LIFT_MARKET_CAP'] = df['LIFT_MARKET_CAP'].astype(np.float64)
    df['FREE_RATIO'] = df['FREE_RATIO'].astype(np.float64)
    df['NEW'] = df['NEW'].astype(np.float64)
    df['TOTAL_RATIO'] = df['TOTAL_RATIO'].astype(np.float64)
    df['NON_FREE_SHARES'] = df['NON_FREE_SHARES'].astype(np.float64)
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 100)
    df.to_sql("T_EM_BJHY_RESTRICTED_RELEASE", engine, index=False, chunksize=500, if_exists='append')
    return df


def func_del_free_date_data(tradedate):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"DELETE FROM  T_EM_BJHY_RESTRICTED_RELEASE WHERE FREE_DATE=to_date({tradedate}, 'yyyy/mm/dd')")
    logger.info(
        f"DELETE FROM  T_EM_BJHY_RESTRICTED_RELEASE WHERE FREE_DATE=to_date({tradedate}, 'yyyy/mm/dd')")

    conn.commit()
    cursor.close()
    logger.info(f"cursor.close()")
    conn.close()
    logger.info(f"conn.close()")


def restricted_release_change(tradedate: str = datetime.datetime.now().strftime("%Y%m%d"), df_em=None):
    try:
        sql = f"""
            SELECT SECURITY_CODE,
            SECURITY_NAME_ABBR AS SECURITY_NAME_ABBR_V2,
            FREE_DATE AS FREE_DATE_V2,
            CURRENT_FREE_SHARES AS CURRENT_FREE_SHARES_V2,
            ABLE_FREE_SHARES AS ABLE_FREE_SHARES_V2,
            FREE_RATIO AS FREE_RATIO_V2,
            FREE_SHARES_TYPE AS FREE_SHARES_TYPE_V2 
            FROM  T_EM_BJHY_RESTRICTED_RELEASE WHERE FREE_DATE=to_date({tradedate}, 'yyyy/mm/dd')
        """
        df_list = pd.read_sql(sql, engine, chunksize=500)
        dflist = []
        for chunk in df_list:
            dflist.append(chunk)
        df_db = pd.concat(dflist)
        df_db.columns = ["SECURITY_CODE", "SECURITY_NAME_ABBR", "FREE_DATE", "CURRENT_FREE_SHARES",
                         "ABLE_FREE_SHARES", "FREE_RATIO", "FREE_SHARES_TYPE"]
        df_em['FREE_DATE'] = pd.to_datetime(df_em['FREE_DATE'])
        diff = df_em.merge(df_db, on=["SECURITY_CODE", "SECURITY_NAME_ABBR", "FREE_DATE", "CURRENT_FREE_SHARES",
                                      "ABLE_FREE_SHARES", "FREE_RATIO", "FREE_SHARES_TYPE"],
                           how='outer', suffixes=('_table1', '_table2'), indicator=True)
        diff = diff[diff['_merge'] != 'both']
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('max_colwidth', 100)
        diff.rename(columns={"_merge": "CHANGE_FLAG"}, inplace=True)
        diff.drop(columns=["LIFT_MARKET_CAP", "NEW", "B20_ADJCHRATE", "A20_ADJCHRATE", "TOTAL_RATIO", "NON_FREE_SHARES",
                           "BATCH_HOLDER_NUM"],
                  inplace=True)
        diff["CHANGE_FLAG"].replace({"left_only": '变化后', "right_only": "变化前"}, inplace=True)
        diff["STOCKCODE"] = diff["SECURITY_CODE"].apply(stock_code)
        diff['CURRENT_FREE_SHARES'] = diff['CURRENT_FREE_SHARES'].astype(np.float64)
        diff['ABLE_FREE_SHARES'] = diff['ABLE_FREE_SHARES'].astype(np.float64)
        diff['FREE_RATIO'] = diff['FREE_RATIO'].astype(np.float64)
        if not diff.empty:
            diff.to_sql("T_EM_BJHY_RESTRICTED_RELEASE_CHANGE", engine, index=False, chunksize=500, if_exists='append')
    except Exception as e:
        logger.error(e)


@logger.catch
def call_back_em_restricted_release(jQuery, find_date, task):
    if task.result() is None:
        logger.error(f"task:{None}")
    else:
        data, status = task.result()
        logger.info(status)
        if status in [200, 201]:
            try:
                d = re.findall(r"jQuery\d+_\d+\((.*)\)", data)
                if len(d) > 0:
                    jdata = json.loads(d[0])
                    if jdata.get("result"):
                        temp_df = pd.DataFrame(jdata["result"]["data"])
                        restricted_release_change(find_date, temp_df)
                        func_del_free_date_data(find_date)
                        save_df_data(temp_df)
                    else:
                        logger.info(jdata)

            except Exception as e:
                logger.error(e)
        else:
            logger.error("status:ERROR")


@logger.catch
def call_back_em_restricted_release_history(jQuery, task):
    if task.result() is None:
        logger.error(f"task:{None}")
    else:
        data, status = task.result()
        logger.info(status)
        if status in [200, 201]:
            try:
                d = re.findall(r"jQuery\d+_\d+\((.*)\)", data)
                if len(d) > 0:
                    jdata = json.loads(d[0])
                    temp_df = pd.DataFrame(jdata["result"]["data"])
                    logger.info(temp_df.shape)
                    save_df_data(temp_df)

            except Exception as e:
                logger.error(e)
        else:
            logger.error("status:ERROR")


@logger.catch
async def fetch_em_network(session, url, headers, data, ip=None):
    try:
        await asyncio.sleep(0.5)
        async with session.get(url, params=data, headers=headers, verify_ssl=False, timeout=10, proxy=None) as response:
            # print(await response.text())
            return await response.text(), response.status
    except Exception as e:
        logger.error(e)


async def network_loop_em_restricted_release(url, find_date):
    tasks = []
    try:
        logger.info(find_date)
        jq = re.sub('\D', '', '1.12.3' + str(random.random()))
        tm = int(time.time() * 1000)
        connector = aiohttp.TCPConnector(ssl=False, limit=0)
        async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
            headers = {
                "sec-ch-ua": '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Sec-Fetch-Dest:": "script",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
            }
            data = {
                "callback": f"jQuery{jq}_{tm}",
                "sortColumns": "FREE_DATE,CURRENT_FREE_SHARES",
                "sortTypes": "1,1",
                "pageSize": 5000,
                "pageNumber": 1,
                "reportName": "RPT_LIFT_STAGE",
                "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,FREE_DATE,CURRENT_FREE_SHARES,ABLE_FREE_SHARES,LIFT_MARKET_CAP,FREE_RATIO,NEW,B20_ADJCHRATE,A20_ADJCHRATE,FREE_SHARES_TYPE,TOTAL_RATIO,NON_FREE_SHARES,BATCH_HOLDER_NUM",
                "source": "WEB",
                "client": "WEB",
                "filter": f"(FREE_DATE>='{find_date}')(FREE_DATE<='{find_date}')",
            }
            await asyncio.sleep(1)
            task = asyncio.ensure_future(fetch_em_network(session, url, headers, data))
            find_date = datetime.datetime.strptime(find_date, "%Y-%m-%d").strftime("%Y%m%d")
            task.add_done_callback(
                functools.partial(call_back_em_restricted_release, f"jQuery{jq}_{tm}((.*));", find_date))
            tasks.append(task)
            await asyncio.wait(tasks)
    except Exception as e:
        logger.error(e)


async def network_loop_em_restricted_release_history(url, start_date, end_date):
    tasks = []
    try:
        headers = {
            "sec-ch-ua": '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Sec-Fetch-Dest:": "script",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        }
        data = {
            "callback": "",
            "sortColumns": "FREE_DATE,CURRENT_FREE_SHARES",
            "sortTypes": "1,1",
            "pageSize": 5000,
            "pageNumber": 1,
            "reportName": "RPT_LIFT_STAGE",
            "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,FREE_DATE,CURRENT_FREE_SHARES,ABLE_FREE_SHARES,LIFT_MARKET_CAP,FREE_RATIO,NEW,B20_ADJCHRATE,A20_ADJCHRATE,FREE_SHARES_TYPE,TOTAL_RATIO,NON_FREE_SHARES,BATCH_HOLDER_NUM",
            "source": "WEB",
            "client": "WEB",
            "filter": f"(FREE_DATE>='{start_date}')(FREE_DATE<='{end_date}')",
        }

        connector = aiohttp.TCPConnector(ssl=False, limit=0)
        async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
            for page in range(1, 65 + 1):
                jq = re.sub('\D', '', '1.12.3' + str(random.random()))
                tm = int(time.time() * 1000)
                data.update({"callback": f"jQuery{jq}_{tm}", "pageNumber": page})
                await asyncio.sleep(1)
                task = asyncio.ensure_future(fetch_em_network(session, url, headers, data))
                task.add_done_callback(
                    functools.partial(call_back_em_restricted_release_history, f"jQuery{jq}_{tm}((.*));"))
                tasks.append(task)
            await asyncio.wait(tasks)
    except Exception as e:
        logger.error(e)


async def main():
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    find_date: str = datetime.datetime.now().strftime("%Y-%m-%d")
    task2 = asyncio.ensure_future(network_loop_em_restricted_release(url, find_date))
    await task2


async def main_part():
    tasks = []
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    now_td = datetime.date.today()
    next_month_td = now_td + relativedelta(months=1)
    now_td = now_td.strftime("%Y-%m-%d")
    next_month_td = next_month_td.strftime("%Y-%m-%d")
    date_range = pd.date_range(start=now_td, end=next_month_td, freq='D')
    date_range = date_range.tolist()
    for find_date in date_range[:]:
        find_date = find_date.strftime("%Y-%m-%d")
        task2 = asyncio.ensure_future(network_loop_em_restricted_release(url, find_date))
        await asyncio.sleep(1)
        tasks.append(task2)
    await asyncio.wait(tasks)


async def main_history():
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    start_date = "2010-01-01"
    end_date = "2033-01-31"
    task1 = asyncio.ensure_future(network_loop_em_restricted_release_history(url, start_date, end_date))
    await task1


@scheduler.scheduled_job("cron", day_of_week="0-4", hour="8", minute="10,45", second="5", id='task106',
                         max_instances=3,
                         misfire_grace_time=10, timezone=shanghai)
@scheduler.scheduled_job("cron", day_of_week="0-4", hour="12", minute="5,10,20", second="5", id='task105',
                         max_instances=3,
                         misfire_grace_time=10, timezone=shanghai)
@scheduler.scheduled_job("cron", day_of_week="0-4", hour="15", minute="10,20,30,40,50", second="5", id='task102',
                         max_instances=3,
                         misfire_grace_time=10, timezone=shanghai)
def job_second():
    asyncio.run(main())


@scheduler.scheduled_job("cron", day_of_week="0-4", hour="8,12", minute="20", second="5", id='task104',
                         max_instances=3,
                         misfire_grace_time=10, timezone=shanghai)
@scheduler.scheduled_job("cron", day_of_week="0-4", hour="16", minute="10", second="5", id='task103',
                         max_instances=3,
                         misfire_grace_time=10, timezone=shanghai)
def job_third():
    asyncio.run(main_part())


class Resticted(Base):
    __tablename__ = "T_EM_BJHY_RESTRICTED_RELEASE"
    __table_args__ = {'comment': '东财个股解禁明细表'}
    rrid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    SECURITY_CODE = Column(String(10), nullable=True, comment="股票代码", index=True)
    stockcode = Column(String(20), nullable=True, comment="股票代码", index=True)
    SECURITY_NAME_ABBR = Column(String(100), nullable=True, comment="股票简称", index=True)
    FREE_DATE = Column(DateTime, nullable=True, comment="解禁日期", index=True)
    CURRENT_FREE_SHARES = Column(Float, nullable=True, comment="实际解禁数量(万股)")
    ABLE_FREE_SHARES = Column(Float, nullable=True, comment="解禁数量(万股)")
    LIFT_MARKET_CAP = Column(Float, nullable=True, comment="实际解禁市值(万元)")
    FREE_RATIO = Column(Float, nullable=True, comment="占解禁前流通市值比例")
    NEW = Column(Float, nullable=True, comment="解禁前一交易日收盘价(元)")
    B20_ADJCHRATE = Column(Float, nullable=True, comment="解禁前20日涨跌幅(%)")
    A20_ADJCHRATE = Column(Float, nullable=True, comment="解禁后20日涨跌幅(%)")
    FREE_SHARES_TYPE = Column(String(200), nullable=True, comment="限售股类型")
    TOTAL_RATIO = Column(Float, nullable=True, comment="TOTAL_RATIO")
    NON_FREE_SHARES = Column(Float, nullable=True, comment="NON_FREE_SHARES")
    BATCH_HOLDER_NUM = Column(Integer, nullable=True, comment="BATCH_HOLDER_NUM")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    def __repr__(self):
        return f"rrid:{self.rrid},SECURITY_CODE:{self.SECURITY_CODE},SECURITY_NAME_ABBR:{self.SECURITY_NAME_ABBR},FREE_DATE:{self.FREE_DATE}," \
               f"CURRENT_FREE_SHARES:{self.CURRENT_FREE_SHARES},LIFT_MARKET_CAP:{self.LIFT_MARKET_CAP},FREE_RATIO:{self.FREE_RATIO},ctime:{self.ctime}"

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


class RestictedChange(Base):
    __tablename__ = "T_EM_BJHY_RESTRICTED_RELEASE_CHANGE"
    __table_args__ = {'comment': '东财个股解禁变化表'}
    rcid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    SECURITY_CODE = Column(String(10), nullable=True, comment="股票代码", index=True)
    STOCKCODE = Column(String(20), nullable=True, comment="股票代码", index=True)
    SECURITY_NAME_ABBR = Column(String(100), nullable=True, comment="股票简称", index=True)
    FREE_DATE = Column(DateTime, nullable=True, comment="解禁日期", index=True)
    CURRENT_FREE_SHARES = Column(Float, nullable=True, comment="实际解禁数量(万股))")
    ABLE_FREE_SHARES = Column(Float, nullable=True, comment="解禁数量(万股)")
    FREE_RATIO = Column(Float, nullable=True, comment="占解禁前流通市值比例")
    FREE_SHARES_TYPE = Column(String(100), nullable=True, comment="限售股类型")
    CHANGE_FLAG = Column(String(15), nullable=True, comment="变化标识")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    def __repr__(self):
        return f"rcid:{self.rcid},SECURITY_CODE:{self.SECURITY_CODE},SECURITY_NAME_ABBR:{self.SECURITY_NAME_ABBR},FREE_DATE:{self.FREE_DATE}," \
               f"CURRENT_FREE_SHARES:{self.CURRENT_FREE_SHARES},LIFT_MARKET_CAP:{self.LIFT_MARKET_CAP},FREE_RATIO:{self.FREE_RATIO},ctime:{self.ctime}"

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


if __name__ == "__main__":
    # asyncio.run(main())
    # asyncio.run(main_part())
    # asyncio.run(main_history())
    # database_init()
    scheduler.start()
