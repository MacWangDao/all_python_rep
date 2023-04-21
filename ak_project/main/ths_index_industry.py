#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2022/2/12 16:11
Desc: 问财-二级行业,涨幅,流通股
http://www.iwencai.com/unifiedwap/home/index
"""
import datetime
from concurrent.futures import ThreadPoolExecutor

import cx_Oracle
import numpy as np
import pandas as pd
import requests
from loguru import logger
from py_mini_racer import py_mini_racer
from apscheduler.schedulers.blocking import BlockingScheduler
import warnings
import os
from sqlalchemy import create_engine
import traceback

from write_influxdb_realtime import realtime_to_influxdb
from write_oracle_realtime import realtime_to_oracle

warnings.filterwarnings("ignore")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=True)
scheduler = BlockingScheduler(timezone='Asia/Shanghai')


def stock_rank(s):
    y = np.linspace(s.min(), s.max(), 8)
    return pd.cut(x=s, bins=y, right=True, labels=[-3, -2, -1, 0, 1, 2, 3], include_lowest=True)


def _get_file_content_ths(file: str = "ths.js") -> str:
    current_path = os.path.dirname(os.path.abspath(__file__))
    # father_path = os.path.dirname(current_path)
    path = os.path.join(current_path, file)

    """
    获取 JS 文件的内容
    :param file:  JS 文件名
    :type file: str
    :return: 文件内容
    :rtype: str
    """

    with open(path) as f:
        file_data = f.read()
    return file_data


def rank_index_industry(date: str = datetime.datetime.now().strftime("%Y%m%d")) -> pd.DataFrame:
    """
    问财-二级行业,涨幅,流通股
    :param date: 查询日期
    :type date: str
    :return: 二级行业,涨幅,流通股
    :rtype: pandas.DataFrame
    """
    url = "http://www.iwencai.com/unifiedwap/unified-wap/v2/result/get-robot-data"
    js_code = py_mini_racer.MiniRacer()
    js_content = _get_file_content_ths("ths.js")
    js_code.eval(js_content)
    v_code = js_code.call("v")
    headers = {
        "hexin-v": v_code,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    }
    params = {
        "question": f"{date} 二级行业,涨幅,流通股",
        "perpage": "100",
        "page": "1",
        "secondary_intent": "zhishu",
        "log_info": '{"input_type":"typewrite"}',
        "source": "Ths_iwencai_Xuangu",
        "version": "2.0",
        "query_area": "",
        "block_list": "",
        # "rsh": "Ths_iwencai_Xuangu_hst1pkx5cuv8giz4dpoy6ay08ccl2zyt",
        "add_info": '{"urp":{"scene":1,"company":1,"business":1},"contentType":"json","searchInfo":true}',
    }
    r = requests.post(url, data=params, headers=headers)

    data_json = r.json()
    data = data_json["data"]["answer"][0]["txt"][0]["content"]["components"][0]["data"]["datas"]
    temp_df = pd.DataFrame(
        data
    )
    temp_df.drop(
        columns=["指数@同花顺行业指数", "指数@所属同花顺行业级别", "market_code", f"指数@最高价:不复权[{date}]", f"指数@换手率[{date}]", "指数代码",
                 f"指数@成交量[{date}]"],
        inplace=True, errors="ignore")
    temp_df.rename(columns={
        "code": "代码",
        "指数简称": "名称",
        f"指数@成交额[{date}]": "总金额",
        f"指数@总市值[{date}]": "总市值",
        f"指数@流通市值[{date}]": "流通市值",
        f"指数@涨跌幅:前复权[{date}]": "涨幅",
        f"指数@开盘价:不复权[{date}]": "开盘价",
        f"指数@收盘价:不复权[{date}]": "现价"

    }, inplace=True)
    # temp_df.drop(
    #     columns=["现价"],
    #     inplace=True, errors="ignore")
    if "现价" in temp_df.columns:
        df = temp_df.loc[:, ["代码", "名称", "涨幅", "现价", "开盘价", "总金额", "总市值", "流通市值"]].copy()
    else:
        df = temp_df.loc[:, ["代码", "名称", "涨幅", "开盘价", "总金额", "总市值", "流通市值"]].copy()
        df["现价"] = df["开盘价"]
        df = df.loc[:, ["代码", "名称", "涨幅", "现价", "开盘价", "总金额", "总市值", "流通市值"]]
    df["涨幅"] = df["涨幅"].astype(np.float64)
    df["现价"] = df["现价"].astype(np.float64)
    df["开盘价"] = df["开盘价"].astype(np.float64)
    df["总金额"] = df["总金额"].astype(np.float64)
    df["总市值"] = df["总市值"].astype(np.float64)
    df["流通市值"] = df["流通市值"].astype(np.float64)
    r.close()

    return df


def rank_index_industry_time_share(find_date: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                                   ) -> pd.DataFrame:
    """
    问财-二级行业,涨幅,流通股
    :param date: 查询日期
    :type date: str
    :return: 二级行业,涨幅,流通股
    :rtype: pandas.DataFrame
    """
    try:
        # find_date = "20221028 14:59"
        print(find_date)
        url = "http://www.iwencai.com/unifiedwap/unified-wap/v2/result/get-robot-data"
        # url = "http://search.10jqka.com.cn/customized/chart/get-robot-data"
        # url = "http://www.iwencai.com/customized/chart/get-robot-data"
        js_code = py_mini_racer.MiniRacer()
        js_content = _get_file_content_ths("ths.js")
        js_code.eval(js_content)
        v_code = js_code.call("v")
        headers = {
            "hexin-v": v_code,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        }
        params = {
            "question": f"{find_date} 二级行业,涨幅",
            "perpage": "100",
            "page": 1,
            "secondary_intent": "zhishu",
            "log_info": '{"input_type":"typewrite"}',
            "source": "Ths_iwencai_Xuangu",
            "version": "2.0",
            "query_area": "",
            "block_list": "",
            "rsh": "Ths_iwencai_Xuangu_r2qkucb38k6d1tsndg5xq7881jv2tm30",
            "add_info": '{"urp":{"scene":1,"company":1,"business":1},"contentType":"json","searchInfo":true}',
        }
        r = requests.post(url, data=params, headers=headers)
        data_json = r.json()
        data = data_json["data"]["answer"][0]["txt"][0]["content"]["components"][0]["data"]["datas"]
        # print(data_json)
        temp_df = pd.DataFrame(
            data
        )
        # print(temp_df.columns)
        # print(temp_df)
        temp_df.drop(
            columns=["指数@同花顺行业指数", "指数@所属同花顺行业级别", "market_code", "指数代码"],
            inplace=True)
        # temp_df.rename(columns={
        #     "code": "代码",
        #     "指数简称": "名称",
        #     f"指数@分时涨跌幅:前复权[{find_date}]": "分时涨跌幅",
        #     f"指数@分时成交额[{find_date}]": "分时成交额",
        #     f"指数@分时成交量[{find_date}]": "分时成交量",
        #     f"指数@分时涨跌:不复权[{find_date}]": "分时涨跌"
        #
        # }, inplace=True)

        temp_df.rename(columns={
            "code": "代码",
            "指数简称": "名称",
            f"指数@分时涨跌幅:前复权[{find_date}]": "分时涨跌幅",
            f"指数@分时成交额[{find_date}]": "分时成交额",
            f"指数@分时成交量[{find_date}]": "分时成交量",
            f"指数@分时涨跌:不复权[{find_date}]": "分时涨跌"

        }, inplace=True)

        df = temp_df.loc[:, ["代码", "名称", "分时涨跌幅", "分时涨跌", "分时成交量", "分时成交额"]].copy()
        df.rename(
            columns={"代码": "INDLEVEL2CODE", "名称": "INDLEVEL2NAME", "分时涨跌幅": "TIMESHAREPCHG", "分时涨跌": "TIMESHARECHANGE",
                     "分时成交量": "TIMESHAREVOL", "分时成交额": "TIMESHAREAMOUNT"}, inplace=True)
        df["ENTRYDATETIME"] = datetime.datetime.strptime(find_date, "%Y%m%d %H:%M")
        df["TIMESHAREPCHG"] = df["TIMESHAREPCHG"].astype(np.float64)
        df["TIMESHARECHANGE"] = df["TIMESHARECHANGE"].astype(np.float64)
        df["TIMESHAREVOL"] = df["TIMESHAREVOL"].astype(np.float64)
        df["TIMESHAREAMOUNT"] = df["TIMESHAREAMOUNT"].astype(np.float64)
        y = np.linspace(df["TIMESHAREPCHG"].min(), df["TIMESHAREPCHG"].max(), 8)
        df["GRADE"] = pd.cut(x=df["TIMESHAREPCHG"], bins=y, right=True, labels=[-3, -2, -1, 0, 1, 2, 3],
                             include_lowest=True)
        df["RANK"] = df["TIMESHAREPCHG"].rank(method='min', ascending=False)

        # df.to_sql("T_S_THS_INDUSTRY_TIMESHARE", engine, index=False, chunksize=500, if_exists='append')

        return df
    except:
        traceback.print_exc()
        print("ERROR!!!")


def rank_index_industry_time_share_history(find_date: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                                           ) -> pd.DataFrame:
    """
    问财-二级行业,涨幅,流通股
    :param date: 查询日期
    :type date: str
    :return: 二级行业,涨幅,流通股
    :rtype: pandas.DataFrame
    """
    try:
        url = "http://www.iwencai.com/unifiedwap/unified-wap/v2/result/get-robot-data"
        js_code = py_mini_racer.MiniRacer()
        js_content = _get_file_content_ths("ths.js")
        js_code.eval(js_content)
        v_code = js_code.call("v")
        headers = {
            "hexin-v": v_code,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        }
        params = {
            "question": f"{find_date} 二级行业,涨幅",
            "perpage": "100",
            "page": "1",
            "secondary_intent": "zhishu",
            "log_info": '{"input_type":"typewrite"}',
            "source": "Ths_iwencai_Xuangu",
            "version": "2.0",
            "query_area": "",
            "block_list": "",
            # "rsh": "Ths_iwencai_Xuangu_hst1pkx5cuv8giz4dpoy6ay08ccl2zyt",
            "add_info": '{"urp":{"scene":1,"company":1,"business":1},"contentType":"json","searchInfo":true}',
        }
        r = requests.post(url, data=params, headers=headers)
        data_json = r.json()
        data = data_json["data"]["answer"][0]["txt"][0]["content"]["components"][0]["data"]["datas"]
        temp_df = pd.DataFrame(
            data
        )

        temp_df.drop(
            columns=["指数@同花顺行业指数", "指数@所属同花顺行业级别", "market_code", "指数代码"],
            inplace=True)
        temp_df.rename(columns={
            "code": "代码",
            "指数简称": "名称",
            f"指数@分时涨跌幅:前复权[{find_date}]": "分时涨跌幅",
            f"指数@分时成交额[{find_date}]": "分时成交额",
            f"指数@分时成交量[{find_date}]": "分时成交量",
            f"指数@分时涨跌:不复权[{find_date}]": "分时涨跌"

        }, inplace=True)

        df = temp_df.loc[:, ["代码", "名称", "分时涨跌幅", "分时涨跌", "分时成交量", "分时成交额"]].copy()
        df.rename(
            columns={"代码": "INDLEVEL2CODE", "名称": "INDLEVEL2NAME", "分时涨跌幅": "TIMESHAREPCHG", "分时涨跌": "TIMESHARECHANGE",
                     "分时成交量": "TIMESHAREVOL", "分时成交额": "TIMESHAREAMOUNT"}, inplace=True)
        df["ENTRYDATETIME"] = datetime.datetime.strptime(find_date, "%Y%m%d %H:%M")
        df["TIMESHAREPCHG"] = df["TIMESHAREPCHG"].astype(np.float64)
        df["TIMESHARECHANGE"] = df["TIMESHARECHANGE"].astype(np.float64)
        df["TIMESHAREVOL"] = df["TIMESHAREVOL"].astype(np.float64)
        df["TIMESHAREAMOUNT"] = df["TIMESHAREAMOUNT"].astype(np.float64)
        df.to_sql("T_S_THS_INDUSTRY_TIMESHARE", engine, index=False, chunksize=500, if_exists='append')

        return df
    except:
        traceback.print_exc()
        print("ERROR!!!")


def rank_index_industry_time_share_history_influxdb(find_date: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                                                    ) -> pd.DataFrame:
    """
    问财-二级行业,涨幅,流通股
    :param date: 查询日期
    :type date: str
    :return: 二级行业,涨幅,流通股
    :rtype: pandas.DataFrame
    """
    try:
        url = "http://www.iwencai.com/unifiedwap/unified-wap/v2/result/get-robot-data"
        js_code = py_mini_racer.MiniRacer()
        js_content = _get_file_content_ths("ths.js")
        js_code.eval(js_content)
        v_code = js_code.call("v")
        headers = {
            "hexin-v": v_code,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        }
        params = {
            "question": f"{find_date} 二级行业,涨幅",
            "perpage": "100",
            "page": "1",
            "secondary_intent": "zhishu",
            "log_info": '{"input_type":"typewrite"}',
            "source": "Ths_iwencai_Xuangu",
            "version": "2.0",
            "query_area": "",
            "block_list": "",
            # "rsh": "Ths_iwencai_Xuangu_hst1pkx5cuv8giz4dpoy6ay08ccl2zyt",
            "add_info": '{"urp":{"scene":1,"company":1,"business":1},"contentType":"json","searchInfo":true}',
        }
        r = requests.post(url, data=params, headers=headers)
        data_json = r.json()
        data = data_json["data"]["answer"][0]["txt"][0]["content"]["components"][0]["data"]["datas"]
        temp_df = pd.DataFrame(
            data
        )

        temp_df.drop(
            columns=["指数@同花顺行业指数", "指数@所属同花顺行业级别", "market_code", "指数代码"],
            inplace=True)
        temp_df.rename(columns={
            "code": "代码",
            "指数简称": "名称",
            f"指数@分时涨跌幅:前复权[{find_date}]": "分时涨跌幅",
            f"指数@分时成交额[{find_date}]": "分时成交额",
            f"指数@分时成交量[{find_date}]": "分时成交量",
            f"指数@分时涨跌:不复权[{find_date}]": "分时涨跌"

        }, inplace=True)

        df = temp_df.loc[:, ["代码", "名称", "分时涨跌幅", "分时涨跌", "分时成交量", "分时成交额"]].copy()
        df.rename(
            columns={"代码": "INDLEVEL2CODE", "名称": "INDLEVEL2NAME", "分时涨跌幅": "TIMESHAREPCHG", "分时涨跌": "TIMESHARECHANGE",
                     "分时成交量": "TIMESHAREVOL", "分时成交额": "TIMESHAREAMOUNT"}, inplace=True)
        df["ENTRYDATETIME"] = datetime.datetime.strptime(find_date, "%Y%m%d %H:%M")
        df["TIMESHAREPCHG"] = df["TIMESHAREPCHG"].astype(np.float64)
        df["TIMESHARECHANGE"] = df["TIMESHARECHANGE"].astype(np.float64)
        df["TIMESHAREVOL"] = df["TIMESHAREVOL"].astype(np.float64)
        df["TIMESHAREAMOUNT"] = df["TIMESHAREAMOUNT"].astype(np.float64)
        y = np.linspace(df["TIMESHAREPCHG"].min(), df["TIMESHAREPCHG"].max(), 8)
        df["GRADE"] = pd.cut(x=df["TIMESHAREPCHG"], bins=y, right=True, labels=[-3, -2, -1, 0, 1, 2, 3],
                             include_lowest=True)
        df["RANK"] = df["TIMESHAREPCHG"].rank(method='min', ascending=False)
        return df
    except:
        traceback.print_exc()
        print("ERROR!!!")


@scheduler.scheduled_job("cron", day_of_week="0-4", hour="10", minute="*/1", second="30", id='task1',
                         max_instances=10)
def job_first():
    find_date = (datetime.datetime.now() + datetime.timedelta(minutes=-1)).strftime("%Y%m%d %H:%M")
    # find_date = datetime.datetime.now().strftime("%Y%m%d %H:%M")
    logger.info(find_date)
    df = rank_index_industry_time_share(find_date=find_date)
    if df is not None:
        with ThreadPoolExecutor(max_workers=3) as pool:
            future1 = pool.submit(realtime_to_oracle, df)
            future2 = pool.submit(realtime_to_influxdb, df)


@scheduler.scheduled_job("cron", day_of_week="0-4", hour="13-14", minute="*/1", second="30", id='task2',
                         max_instances=10)
def job_second():
    # find_date = datetime.datetime.now().strftime("%Y%m%d %H:%M")
    find_date = (datetime.datetime.now() + datetime.timedelta(minutes=-1)).strftime("%Y%m%d %H:%M")
    logger.info(find_date)
    df = rank_index_industry_time_share(find_date=find_date)
    if df is not None:
        with ThreadPoolExecutor(max_workers=3) as pool:
            future1 = pool.submit(realtime_to_oracle, df)
            future2 = pool.submit(realtime_to_influxdb, df)


@scheduler.scheduled_job("cron", day_of_week="0-4", hour="11", minute="0-31/1", second="30", id='task3',
                         max_instances=10)
def job_third():
    # find_date = datetime.datetime.now().strftime("%Y%m%d %H:%M")
    find_date = (datetime.datetime.now() + datetime.timedelta(minutes=-1)).strftime("%Y%m%d %H:%M")
    logger.info(find_date)
    df = rank_index_industry_time_share(find_date=find_date)
    if df is not None:
        with ThreadPoolExecutor(max_workers=3) as pool:
            future1 = pool.submit(realtime_to_oracle, df)
            future2 = pool.submit(realtime_to_influxdb, df)


@scheduler.scheduled_job("cron", day_of_week="0-4", hour="15", minute="1", second="30", id='task4', max_instances=10)
def job_forth():
    # find_date = datetime.datetime.now().strftime("%Y%m%d %H:%M")
    find_date = (datetime.datetime.now() + datetime.timedelta(minutes=-1)).strftime("%Y%m%d %H:%M")
    logger.info(find_date)
    df = rank_index_industry_time_share(find_date=find_date)
    if df is not None:
        with ThreadPoolExecutor(max_workers=3) as pool:
            future1 = pool.submit(realtime_to_oracle, df)
            future2 = pool.submit(realtime_to_influxdb, df)


@scheduler.scheduled_job("cron", day_of_week="0-4", hour="9", minute="30-59/1", second="30", id='task5',
                         max_instances=10)
def job_fifth():
    # find_date = datetime.datetime.now().strftime("%Y%m%d %H:%M")
    find_date = (datetime.datetime.now() + datetime.timedelta(minutes=-1)).strftime("%Y%m%d %H:%M")
    logger.info(find_date)
    df = rank_index_industry_time_share(find_date=find_date)
    if df is not None:
        with ThreadPoolExecutor(max_workers=3) as pool:
            future1 = pool.submit(realtime_to_oracle, df)
            future2 = pool.submit(realtime_to_influxdb, df)


if __name__ == "__main__":
    scheduler.start()
