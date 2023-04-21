import copy
import functools
import time

import requests
import cx_Oracle
import pandas as pd
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
from sqlalchemy import create_engine
import numpy as np
import traceback
import os
import datetime
import warnings
import aiohttp
import asyncio
import platform
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from apscheduler.schedulers.blocking import BlockingScheduler

# scheduler = AsyncIOScheduler(timezone='Asia/Shanghai')
scheduler = BlockingScheduler(timezone='Asia/Shanghai')
try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError as e:
    logger.exception(e)

logger.add("log/hkexnews-0418.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")
warnings.filterwarnings("ignore")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
# engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=False)
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='dazh')  # dsn
engine = create_engine("oracle://fcdb:fcdb@" + dns, encoding='utf-8', echo=False)  # 建立ORM连接

completed_count = 0
fail_ccass_list = []
error_count = 0
total_code_count = 0


@logger.catch
def get_tradedate_rownum(tradedate: str = None):
    try:
        tradedate_sql = "SELECT to_char(tr.ENTRYDATETIME,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  ENTRYDATETIME FROM T_S_LUGUTONG_STOCKS ORDER BY ENTRYDATETIME desc) tr WHERE rownum<=1"
        if tradedate:
            tradedate_sql = f"SELECT to_char(tr.ENTRYDATETIME,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  ENTRYDATETIME FROM T_S_LUGUTONG_STOCKS ORDER BY ENTRYDATETIME desc) tr WHERE ENTRYDATETIME = to_date({tradedate}, 'yyyy/mm/dd') AND rownum<=1"
        df_tradedate = pd.read_sql(tradedate_sql, engine)
        tradedate_list = sorted(df_tradedate["tradeday"].tolist())
        if len(tradedate_list) == 1:
            return tradedate_list[0]
    except Exception as e:
        logger.exception(e)


@logger.catch
def oracle_ccasscode(tradedate):
    ccass_df_list = pd.read_sql(
        f"select CCASSCODE,STOCKCODE,STOCKNAME from T_S_LUGUTONG_STOCKS WHERE ENTRYDATETIME = to_date({tradedate}, 'yyyy/mm/dd')",
        engine,
        chunksize=5000)
    dflist = []
    for chunk in ccass_df_list:
        dflist.append(chunk)
    ccass_df = pd.concat(dflist)
    ccasscode_dict = ccass_df.to_dict(orient='records')
    # ccasscode_list = ccasscode_dict.get("ccasscode", [])
    return ccasscode_dict


@logger.catch
def oracle_rep_ccasscode(tradedate):
    ccass_df_list = pd.read_sql(
        f"SELECT DISTINCT  CCASSCODE from T_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY = to_date({tradedate}, 'yyyy/mm/dd')",
        engine,
        chunksize=5000)
    dflist = []
    for chunk in ccass_df_list:
        dflist.append(chunk)
    ccass_df = pd.concat(dflist)
    ccasscode_dict = ccass_df.to_dict(orient='list')
    ccasscode_list = ccasscode_dict.get("ccasscode", [])
    return ccasscode_list


def set_headers():
    headers = {
        "authority": "www3.hkexnews.hk",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "cache-control": "max-age=0",
        'content-type': 'application/x-www-form-urlencoded',
        'cookie': 'sclang=zh-HK; WT_FPC=id=23.43.249.170-843578560.30970140:lv=1657695841691:ss=1657695841691',
        # 'origin': 'https://www3.hkexnews.hk',
        'origin': 'https://sc.hkexnews.hk',
        # 'referer': 'https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx',
        'referer': "https://sc.hkexnews.hk/TuniS/www3.hkexnews.hk/sdw/search/searchsdw_c.aspx",
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
        'sec-ch-ua-mobile': '?0',
        "sec-ch-ua-platform": '"Windows"',
        'sec-fetch-dest': "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"
    }
    return headers


def set_data(txtStockCode, txtShareholdingDate=None):
    if not txtShareholdingDate:
        txtShareholdingDate = datetime.datetime.now().strftime("%Y/%m/%d")
    today = datetime.datetime.now().strftime("%Y%m%d")
    data = {"__EVENTTARGET": "btnSearch",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": "/wEPDwUKMTY0ODYwNTA0OWRkM79k2SfZ+VkDy88JRhbk+XZIdM0=",
            "__VIEWSTATEGENERATOR": "3B50BBBD",
            "today": today,
            "sortBy": "shareholding",
            "sortDirection": "desc",
            "alertMsg": "",
            "txtShareholdingDate": txtShareholdingDate,
            "txtStockCode": txtStockCode,
            # "txtStockName": "招商證券",
            "txtParticipantID": "",
            "txtParticipantName": "",
            "txtSelPartID": ""}

    return data


@logger.catch
async def fetch(session, url, headers, data, ip=None):
    try:
        await asyncio.sleep(0.5)
        # raise ("timeout")
        timeout = aiohttp.ClientTimeout(total=60)
        async with session.post(url, data=data, headers=headers, verify_ssl=False, timeout=timeout,
                                proxy=None) as response:
            return await response.text(), response.status
    except asyncio.exceptions.TimeoutError:
        logger.error(f"asyncio.exceptions.TimeoutError:{data.get('txtStockCode')}")
    except Exception as e:
        logger.exception(e)


def read_html_to_oracle(txtStockCodeSet, tradedate_str, text):
    txtStockCode = txtStockCodeSet.get("ccasscode")
    try:
        df = pd.read_html(text, encoding='utf-8', header=0)[0]
        df.drop(columns=['地址'], inplace=True)
        df.rename(columns={'参与者编号': 'BROKERCODE', '中央结算系统参与者名称(*即愿意披露的投资者户口持有人)': 'BROKERNAME', '持股量': 'HOLDNUM',
                           '占于上交所/深交所上市及交易的A股总数的百分比': 'HOLDRATE'}, inplace=True)
        df = df.applymap(lambda x: str(x).split(":")[1].strip())
        # df["持股量"] = df["持股量"].str.replace(',', '').astype(int)
        # df["占于上交所/深交所上市及交易的A股总数的百分比"] = df["占于上交所/深交所上市及交易的A股总数的百分比"].str.replace('%', '').astype(np.float64)
        df["HOLDNUM"] = df["HOLDNUM"].str.replace(',', '').astype(int)
        df["HOLDRATE"] = df["HOLDRATE"].str.replace('%', '').astype(np.float64)
        tradedate = datetime.datetime.strptime(tradedate_str, '%Y%m%d')
        df["TRADEDAY"] = tradedate
        df["STOCKCODE"] = txtStockCodeSet.get("stockcode")
        df["STOCKNAME"] = txtStockCodeSet.get("stockname")
        df["CCASSCODE"] = txtStockCodeSet.get("ccasscode")
        df.to_sql("T_HK_BROKERHOLDNUM_NEW", engine, index=False, chunksize=500, if_exists='append')
        # df.to_csv(f"data_csv/{tradedate_str}/{txtStockCode}.csv", index=False, encoding="utf_8_sig")
        index = txtStockCodeSet.get("index")
        logger.info(f"index:{index},save:{txtStockCode}")
    except ValueError as e:
        logger.error(f"warning:{e},txtStockCode:{txtStockCode}")
    except Exception as e:
        logger.exception(e)
        logger.error(f"warning:{e},txtStockCode:{txtStockCode}")


@logger.catch
def call_back(txtStockCodeSet, tradedate, task):
    global completed_count
    global error_count
    txtStockCode = txtStockCodeSet.get("ccasscode")

    if task.result() is None:
        if txtStockCodeSet not in fail_ccass_list:
            fail_ccass_list.append(txtStockCodeSet)
            error_count += 1
        logger.error(f"txtStockCode:{txtStockCode}")
        # asyncio.ensure_future(status_error(txtStockCode, tradedate))
    else:
        text, status = task.result()
        if status in [200, 201]:
            completed_count += 1
            if txtStockCode in fail_ccass_list:
                fail_ccass_list.remove(txtStockCode)
            try:
                read_html_to_oracle(txtStockCodeSet, tradedate, text)
            except Exception as e:
                logger.exception(e)
        else:
            if txtStockCode not in fail_ccass_list:
                fail_ccass_list.append(txtStockCodeSet)
                error_count += 1
            # asyncio.ensure_future(status_error(txtStockCode, tradedate))
            logger.error(f"txtStockCode:{txtStockCode},status:{status}")


def call_back_error(txtStockCodeSet, tradedate_str, task):
    global completed_count
    global error_count
    txtStockCode = txtStockCodeSet.get("ccasscode")
    if task.result() is None:
        if txtStockCodeSet not in fail_ccass_list:
            fail_ccass_list.append(txtStockCodeSet)
            error_count += 1
        logger.error(f"txtStockCode:{txtStockCode}")
    else:
        text, status = task.result()
        if status in [200, 201]:
            completed_count += 1
            if txtStockCodeSet in fail_ccass_list:
                fail_ccass_list.remove(txtStockCodeSet)
            try:
                read_html_to_oracle(txtStockCodeSet, tradedate_str, text)
            except Exception as e:
                logger.warning(f"warning:{e},txtStockCode:{txtStockCode}")
        else:
            if txtStockCodeSet not in fail_ccass_list:
                fail_ccass_list.append(txtStockCodeSet)
                error_count += 1
            logger.error(f"txtStockCode:{txtStockCode},status:{status}")


def proxy_ip():
    '''
    ip = 'HTTP://121.232.148.17:9000'
    http://183.220.145.3:80
    '''
    return 'http://180.120.210.97:8888'


async def fetch_v2(url, headers, data):
    try:
        await asyncio.sleep(1)
        # raise ("timeout")
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, headers=headers, verify_ssl=False, timeout=timeout,
                                    proxy=None) as response:
                return await response.text(), response.status
    except asyncio.exceptions.TimeoutError:
        logger.error(f"asyncio.exceptions.TimeoutError:{data.get('txtStockCode')}")
    except Exception as e:
        logger.exception(e)


async def check_fail_code(total_code, url, headers, tradedate):
    last_count = 0
    mandatory_finished = 0
    while True:
        logger.info(
            f"check_fail_code->completed_count:{completed_count},total_code:{total_code},fail_ccass_list:{fail_ccass_list},error_count:{error_count},tradedate:{tradedate}")
        await asyncio.sleep(1)
        mandatory_finished += 1
        logger.info(f"mandatory_finished:{mandatory_finished}")
        txtShareholdingDate = datetime.datetime.strptime(tradedate, "%Y%m%d").strftime("%Y/%m/%d")
        for _ in fail_ccass_list[:]:
            code_set = fail_ccass_list.pop()
            txtStockCode = code_set.get("ccasscode")
            data = set_data(txtStockCode, txtShareholdingDate)
            await asyncio.sleep(0.15)
            task = asyncio.ensure_future(fetch_v2(url, headers, data))
            task.add_done_callback(functools.partial(call_back_error, code_set, tradedate))
        if completed_count == total_code and len(fail_ccass_list) == 0:
            last_count += 1
            logger.warning(f"last_count:{last_count}")
            if last_count == 3:
                break
        if mandatory_finished == 400:
            break


async def network_loop_v2(url, tradedate, txtStockCode_list):
    ip = proxy_ip()
    tasks = []
    logger.info(f"total_count:{len(txtStockCode_list)}")
    txtShareholdingDate = datetime.datetime.strptime(tradedate, "%Y%m%d").strftime("%Y/%m/%d")
    os.makedirs(f"data_csv/{tradedate}", exist_ok=True)
    headers = set_headers()
    logger.info(f"save_dir:data_csv/{tradedate}")
    try:
        connector = aiohttp.TCPConnector(ssl=False, limit=100)
        async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
            for index, txtStockCodeSet in enumerate(txtStockCode_list):
                txtStockCodeSet["index"] = index
                txtStockCode = txtStockCodeSet.get("ccasscode")
                logger.info(f"tradedate:{tradedate},index:{index},txtStockCode:{txtStockCode}")
                data = set_data(txtStockCode, txtShareholdingDate)
                await asyncio.sleep(0.05)
                task = asyncio.ensure_future(fetch(session, url, headers, data, ip))
                task.add_done_callback(functools.partial(call_back, txtStockCodeSet, tradedate))
                tasks.append(task)
            await asyncio.wait(tasks)
    except Exception as e:
        logger.exception(e)


def set_mins_db_rep_code(tradedate):
    rep_ccasscode = oracle_rep_ccasscode(tradedate)
    tradedate_ccasscode = get_tradedate_rownum()
    txtStockCode_list = oracle_ccasscode(tradedate_ccasscode)[:]
    for ccasscode_set in txtStockCode_list[:]:
        ccasscode = ccasscode_set.get("ccasscode")
        if ccasscode in rep_ccasscode:
            txtStockCode_list.remove(ccasscode_set)
    return txtStockCode_list


def history_tradedate_last_daily(history_tradeday=None):
    if history_tradeday is not None:
        return history_tradeday
    now = datetime.datetime.now().strftime("%Y%m%d")
    ccass_df_list = pd.read_sql(
        f"SELECT PRETRADE_DATE FROM T_S_TRADECALENDAR WHERE CAL_DATE = to_date({now},'yyyy/mm/dd') AND EXCHANGE='SSE'",
        engine,
        chunksize=500)
    dflist = []
    for chunk in ccass_df_list:
        dflist.append(chunk)
    tradeday_df = pd.concat(dflist)
    tradeday_dict = tradeday_df.to_dict(orient='list')
    tradeday_list = tradeday_dict.get("pretrade_date", [])
    if len(tradeday_list) > 0:
        trade = tradeday_list[0]
        tradedate = trade.strftime("%Y%m%d")
        return tradedate


def func_tradeday_changenum(tradedate):
    ccasscode_list = oracle_rep_ccasscode(tradedate)
    if len(ccasscode_list) > 0:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        completed = cursor.callfunc('FUNC_TRADEDAY_DEL_REP_DATA', cx_Oracle.STRING, [tradedate])
        logger.info(f"tradedate:{tradedate},FUNC_TRADEDAY_DEL_REP_DATA:{completed}")
        completed = cursor.callfunc('FUNC_TRADEDAY_CHANGENUM', cx_Oracle.STRING, [tradedate])
        logger.info(f"tradedate:{tradedate},FUNC_TRADEDAY_CHANGENUM:{completed}")
        completed = cursor.callfunc('FUNC_T_O_HK_BRO_INSERTDATA', cx_Oracle.STRING, [tradedate])
        logger.info(f"tradedate:{tradedate},FUNC_T_O_HK_BRO_INSERTDATA:{completed}")
        logger.info(
            f"SELECT  COUNT(*)  FROM  T_O_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd')")
        cursor.execute(
            f"SELECT  COUNT(*)  FROM  T_O_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd')")
        result = cursor.fetchone()
        logger.info(f"tradedate:{tradedate},T_O_HK_BROKERHOLDNUM_NEW_COUNT:{result}")
        if result:
            if result[0] == 0:
                # cursor.callproc("T_O_HK_lugutongstock")
                time.sleep(15)
                completed = cursor.callfunc('FUNC_T_O_HK_BRO_INSERTDATA', cx_Oracle.STRING, [tradedate])
                logger.info(f"tradedate:{tradedate},FUNC_T_O_HK_BRO_INSERTDATA:{completed}")
            else:
                pass
                # cursor.execute(f"DELETE FROM  T_O_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd')")
                # logger.info(f"DELETE FROM  T_O_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd')")
                # logger.info(f"tradedate:{tradedate},DELETE:T_O_HK_BROKERHOLDNUM_NEW")
                # cursor.callproc("T_O_HK_lugutongstock")
                # logger.info(f"tradedate:{tradedate},T_O_HK_lugutongstock:completed")
        completed = cursor.callfunc('FUNC_T_O_HK_BRO_DEL_REP', cx_Oracle.STRING, [tradedate])
        logger.info(f"tradedate:{tradedate},FUNC_T_O_HK_BRO_DEL_REP:{completed}")
        cursor.close()
        logger.info(f"cursor.close()")
        conn.close()
        logger.info(f"conn.close()")
    else:
        logger.info(f"tradedate:{tradedate},FUNC_TRADEDAY_CHANGENUM:无更新条目")


def func_del_holdnum_zone(tradedate):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"DELETE FROM  T_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd') AND HOLDNUM=0 AND HOLDRATE IS NULL")
    logger.info(
        f"DELETE FROM  T_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd') AND HOLDNUM=0 AND HOLDRATE IS NULL")

    conn.commit()
    cursor.close()
    logger.info(f"cursor.close()")
    conn.close()
    logger.info(f"conn.close()")


# @scheduler.scheduled_job("cron", day_of_week="0-5", second="*/3", id='task1', misfire_grace_time=60, max_instances=10)
async def main(url=None, tradedate=None):
    global completed_count
    global fail_ccass_list
    global error_count
    global total_code_count
    # 20220630
    # https://sc.hkexnews.hk/TuniS/www3.hkexnews.hk/sdw/search/searchsdw_c.aspx
    if not url:
        url = "https://sc.hkexnews.hk/TuniS/www3.hkexnews.hk/sdw/search/searchsdw_c.aspx"
    if not tradedate:
        tradedate = history_tradedate_last_daily()
    func_del_holdnum_zone(tradedate)
    txtStockCode_list = set_mins_db_rep_code(tradedate)
    total_code_count = len(txtStockCode_list)
    headers = set_headers()
    task1 = asyncio.ensure_future(network_loop_v2(url, tradedate, txtStockCode_list))
    await asyncio.sleep(1)
    task2 = asyncio.ensure_future(check_fail_code(total_code_count, url, headers, tradedate))
    await task1
    await task2
    func_tradeday_changenum(tradedate)
    completed_count = 0
    fail_ccass_list = []
    error_count = 0
    total_code_count = 0


# @scheduler.scheduled_job("cron", day_of_week="0-5", hour="2", minute="0", id='task1')
# @scheduler.scheduled_job("cron", day_of_week="0-5", second="*/3", id='task1')
@scheduler.scheduled_job("cron", day_of_week="0-5", hour="2,6", minute=1, second=1, misfire_grace_time=10,
                         max_instances=10,
                         id='spider_hk_01')
def spider_hk_job_first():
    logger.info("run...")

    """
    year：四位数的年份
    month：1-12之间的数字或字符串，如果不指定，则为*，表示每个月
    day：1-31，如果不指定，则为*，表示每一天
    week：1-53，如果不指定，则为*，表示每一星期
    day_of_week：一周有7天，用0-6表示，比如指定0-3，则表示周一到周四。不指定则为7天，也可以用          mon,tue,wed,thu,fri,sat,sun表示
    hour：0-23
    minute：0-59
    second：0-59
    start_date：起始时间
    end_date：结束时间
    timezone：时区
    jitter：随机的浮动秒数
    当省略时间参数时，在显式指定参数之前的参数会被设定为*，表示每(月、天)xxx。之后的参数会被设定为最小值，week 和day_of_week的最小值为*。
    比如，设定day=10等同于设定year='*', month='*', day=1, week='*', day_of_week='*', hour=0, minute=0, second=0，
    即每个月的第10天触发。为什么是每个月而不是每个星期，注意参数位置，week被放在了后面。day后面的参数hour、minute、second则被设置为0。因此不仅是每个月的第10天触发，还是每个月的第10天的00:00:00的时候触发
    """
    asyncio.run(main())


@scheduler.scheduled_job("cron", day_of_week="0-5", hour=2, minute=25, second=1, misfire_grace_time=10,
                         max_instances=10,
                         id='spider_hk_02')
def spider_hk_job_second():
    if total_code_count == 0:
        asyncio.run(main())


if __name__ == "__main__":
    # asyncio.run(main(tradedate="20230411"))
    tradedates = ["20230414", "20230417"]
    for tradedate in tradedates:
        for _ in range(3):
            asyncio.run(main(tradedate=tradedate))
