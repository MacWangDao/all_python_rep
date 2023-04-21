# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 16:08:50 2022

@author: OnePiece
"""
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List

import cx_Oracle as oracle
import time
import os

import functools
import pandas as pd
import pandas
import numpy
import numpy as np
import datetime
import matplotlib.pyplot as plt
import paramiko
from io import BytesIO
import stat

from matplotlib import ticker
from matplotlib.font_manager import FontProperties
from sqlalchemy import create_engine
from loguru import logger
import traceback
from description_img_1012 import stock_header_description

# logger.remove(handler_id=None)
logger.add("./log/excel_file.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

dns = oracle.makedsn('192.168.101.215', 1521, service_name='dazh')  # dsn
engine = create_engine("oracle://fcdb:fcdb@" + dns, encoding='utf-8', echo=True)  # 建立ORM连接

global_data_res = {}


# 查找昨收价
def lclose(tradedate):
    # AVGPRICE 昨收价
    # aaa = "select q.lclose,s.symbol from TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s where q.secode =  s.secode and  s.symbol =  " + symbol + " and q.tradedate = " + tradedate +""
    aaa = "select s.symbol,q.AVGPRICE from TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s where q.secode =  s.secode and q.tradedate = " + tradedate + ""
    # print(aaa)
    conn = oracle.connect('fcdb/fcdb@192.168.101.215:1521/dazh')
    c = conn.cursor()
    c.execute(aaa)
    # res = c.description
    rs = c.fetchall()
    pp = dict(rs)
    # print(len(rs))
    return pp


def TOTMKTCAP(tradedate):
    # TOTMKTCAP 市值
    # aaa = "select q.lclose,s.symbol from TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s where q.secode =  s.secode and  s.symbol =  " + symbol + " and q.tradedate = " + tradedate +""
    aaa = "select s.symbol,q.TOTMKTCAP from TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s where q.secode =  s.secode and q.tradedate = " + tradedate + ""
    # print(aaa)
    conn = oracle.connect('fcdb/fcdb@192.168.101.215:1521/dazh')
    c = conn.cursor()
    c.execute(aaa)
    # res = c.description
    rs = c.fetchall()
    tt = dict(rs)
    # print(len(rs))
    return tt


# 整理需求数据
def forRS(fordate):
    aaa = "select brokercode,brokername,stockcode,stockname,changenum,industryname  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd')"
    conn = oracle.connect('fcdb/fcdb@192.168.101.215:1521/dazh')
    c = conn.cursor()
    c.execute(aaa)
    # res = c.description
    rs = c.fetchall()
    return rs


def recordA(fordate):
    # 说明
    a1 = "select count(1)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') "
    a60 = "select count(1)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '60%'"
    a00 = "select count(1)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '00%'"
    a30 = "select count(1)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '30%'"
    a688 = "select count(1)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '688%'"
    conn = oracle.connect('fcdb/fcdb@192.168.101.215:1521/dazh')
    c = conn.cursor()
    c.execute(a1)
    rs = c.fetchall()
    c.execute(a60)
    rs = c.fetchall()[0] + rs[0]
    c.execute(a00)
    rs = c.fetchall()[0] + rs
    c.execute(a30)
    rs = c.fetchall()[0] + rs
    c.execute(a688)
    rs = c.fetchall()[0] + rs
    conn.close()

    b1 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd')  and changenum > 0 group by stockcode,stockname"
    b60 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '60%' and changenum > 0 group by stockcode,stockname"
    b00 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '00%' and changenum > 0 group by stockcode,stockname"
    b30 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '30%' and changenum > 0 group by stockcode,stockname"
    b688 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '688%' and changenum > 0 group by stockcode,stockname"
    conn = oracle.connect('fcdb/fcdb@192.168.101.215:1521/dazh')
    ss = lclose(str(fordate))

    c = conn.cursor()
    c.execute(b1)
    rsbuy = c.fetchall()
    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    b1 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    # c.execute(b00)

    c.execute(b60)
    rsbuy = c.fetchall()
    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    b60 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    c.execute(b00)
    rsbuy = c.fetchall()
    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    b00 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    c.execute(b30)
    rsbuy = c.fetchall()
    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    b30 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    c.execute(b688)
    rsbuy = c.fetchall()
    # rsbuy688 = []
    # for i in range(len(rsbuy)):
    #     # rsbuy688 = rsbuy[i][2] + rsbuy688
    #     print(rsbuy[i][2])

    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    b688 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    # print(b688)
    # rs = paqc.groupby(by=[0,1])
    bbuy = list((b688, b30, b00, b60, b1))
    # bbuy = bbuy + b688

    s1 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd')  and changenum < 0 group by stockcode,stockname"
    s60 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '60%' and changenum < 0 group by stockcode,stockname"
    s00 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '00%' and changenum < 0 group by stockcode,stockname"
    s30 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '30%' and changenum < 0 group by stockcode,stockname"
    s688 = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') and stockcode like '688%' and changenum < 0 group by stockcode,stockname"
    conn = oracle.connect('fcdb/fcdb@192.168.101.215:1521/dazh')

    c = conn.cursor()
    c.execute(s1)
    rsbuy = c.fetchall()
    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    s1 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    # c.execute(s1)

    c.execute(s60)
    rsbuy = c.fetchall()
    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    s60 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    c.execute(s00)
    rsbuy = c.fetchall()
    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    s00 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    c.execute(s30)
    rsbuy = c.fetchall()
    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    s30 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    c.execute(s688)
    rsbuy = c.fetchall()
    # rsbuy688 = []
    # for i in range(len(rsbuy)):
    #     # rsbuy688 = rsbuy[i][2] + rsbuy688
    #     print(rsbuy[i][2])

    for i in range(len(rsbuy)):
        if rsbuy[i][0] in ss:
            nu = ss[rsbuy[i][0]]
        else:
            nu = 0
        fo = int(rsbuy[i][2]) * int(nu)

        rsbuy[i] = rsbuy[i] + (fo,)
        # print(rsbuy[i] , "--------------------")
    s688 = pandas.DataFrame(rsbuy, columns=['股票代码', '股票名称', 'C', fordate])[fordate].sum()
    # print(b688)
    # rs = paqc.groupby(by=[0,1])
    ssell = list((s688, s30, s00, s60, s1))
    networth = list((s688 + b688, s30 + b30, s00 + b00, s60 + b60, s1 + b1))

    forall = {'说明': ['沪科创688', '深创业30', '深主板00', '沪主板60', '汇总'], 'record' + fordate: list(rs), 'buy': bbuy,
              'sell': ssell, 'net worth': networth}
    forall = pandas.DataFrame(forall)
    return forall


def aShards(fordate):
    aaa = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') group by stockcode,stockname"
    conn = oracle.connect('fcdb/fcdb@192.168.101.215:1521/dazh')
    c = conn.cursor()
    c.execute(aaa)
    # res = c.description
    rs = c.fetchall()
    # d = 0
    ss = lclose(str(fordate))
    # print(rs[0][0])
    for i in range(len(rs)):
        # print(rs[i])
        # ss = lclose(str(rs[i][0]),str(20220401))
        if rs[i][0] in ss:
            nu = ss[rs[i][0]]
        else:
            nu = 0
        # print(ss[rs[i][0]],rs[i][1])
        # fo = list(rs[i])
        fo = int(rs[i][2]) * int(nu)
        # print(str(fo))

        rs[i] = rs[i] + (fo, nu)
        # print(i , "--------------------")
    paqc = pandas.DataFrame(rs, columns=['股票代码', '股票名称', 'C', fordate, 'E']).set_index(['股票名称']).sort_values(
        by=('股票名称'), ascending=False, inplace=False)
    paqc[fordate].fillna(0)
    # rs = paqc.groupby(by=[0,1])

    return (paqc)


def aShardslv_sa(fordate):
    aaa = "select stockcode,stockname,sum(changenum)  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd') group by stockcode,stockname"
    conn = oracle.connect('fcdb/fcdb@192.168.101.215:1521/dazh')
    c = conn.cursor()
    c.execute(aaa)
    # res = c.description
    rs = c.fetchall()
    # d = 0
    ss = lclose(str(fordate))
    tt = TOTMKTCAP(str(fordate))
    # print(rs[0][0])
    for i in range(len(rs)):
        # print(rs[i])
        # ss = lclose(str(rs[i][0]),str(20220401))
        if rs[i][0] in ss:
            nu = ss[rs[i][0]]
            nv = tt[rs[i][0]]
        else:
            nu = 0
            nv = 0
        # print(ss[rs[i][0]],rs[i][1])
        # fo = list(rs[i])
        fo = int(rs[i][2]) * int(nu) / nv * 100 if nv != 0 else 0
        # print(str(fo))

        rs[i] = rs[i] + (fo, nu)
        # print(i , "--------------------")
    paqc = pandas.DataFrame(rs, columns=['股票代码', '股票名称', 'C', fordate, 'E']).set_index(['股票名称']).sort_values(
        by=('股票名称'), ascending=False, inplace=False)
    paqc[fordate].fillna(0)
    # rs = paqc.groupby(by=[0,1])

    return (paqc)


def jingJS(fordate, rs1):
    ss = lclose(str(fordate))
    # print(rs[0][0])
    # 计算额度
    for i in range(len(rs1)):
        # print(rs[i])
        # ss = lclose(str(rs[i][0]),str(20220401))
        if rs1[i][2] in ss:
            nu = ss[rs1[i][2]]
        else:
            nu = 0
        # print(ss[rs[i][0]],rs[i][1])
        # fo = list(rs[i])
        fo = int(rs1[i][4]) * int(nu)
        # print(str(fo))

        rs1[i] = rs1[i] + (fo,)
    # groupd = rs[5]
    paqc1 = pandas.DataFrame(rs1).sort_values(by=6, ascending=False)
    groupd = paqc1.groupby(by=[0, 1])
    rs1 = groupd.sum()
    # print(rs1)

    return rs1


def jingJSGroup(fordate, rs2):
    ss = lclose(str(fordate))
    # 计算额度
    for i in range(len(rs2)):
        # print(rs[i])
        # ss = lclose(str(rs[i][0]),str(20220401))
        if rs2[i][2] in ss:
            nu = ss[rs2[i][2]]
        else:
            nu = 0
        # print(ss[rs[i][0]],rs[i][1])
        # fo = list(rs[i])
        fo = int(rs2[i][4]) * int(nu)
        # print(str(fo))

        rs2[i] = rs2[i] + (fo, nu)
    # groupd = rs[5]
    paqc1 = pandas.DataFrame(rs2).sort_values(by=6, ascending=False)
    groupd = paqc1.groupby(by=[0, 1, 2, 3])
    rs2 = groupd.sum()

    return rs2


def banKuai(fordate):
    # aaa = "select brokercode,brokername,stockcode,stockname,changenum,industryname  from T_O_BrokerAllPosition where tradeday = to_date(" + fordate + ", 'yyyy/mm/dd')"
    # conn = oracle.connect('bjhy/bjhy@192.168.101.215:1521/bjhy')
    # c = conn.cursor()
    # c.execute(aaa)
    # # res = c.description
    # rs = c.fetchall()
    # d = 0
    ss = lclose(str(fordate))
    rs3 = forRS(fordate)
    # print(rs[0][0])
    for i in range(len(rs3)):
        # print(rs3[i])
        # ss = lclose(str(rs[i][0]),str(20220401))
        if rs3[i][2] in ss:
            nu = ss[rs3[i][2]]
        else:
            nu = 0
        # print(ss[rs[i][0]],rs[i][1])
        # fo = list(rs[i])
        fo = int(rs3[i][4]) * int(nu)
        # print(str(fo))

        rs3[i] = rs3[i] + (fo,)
        # print(i , "--------------------",fordate)
    paqc1 = pandas.DataFrame(rs3, columns=['A', 'B', 'C', 'D', 'E', 'F', fordate]).sort_values(by='F', ascending=False)
    paqc1 = paqc1.drop(labels='A', axis=1)
    paqc1 = paqc1.drop(labels='B', axis=1)
    paqc1 = paqc1.drop(labels='C', axis=1)
    paqc1 = paqc1.drop(labels='D', axis=1)
    paqc1 = paqc1.drop(labels='E', axis=1)
    # paqc1 = paqc1.drop(labels=0 ,axis=1)
    # paqc1 = paqc1.drop(labels=1 ,axis=1)
    # paqc1 = paqc1.drop(labels=2 ,axis=1)
    # paqc1 = paqc1.drop(labels=3 ,axis=1)
    # paqc1 = paqc1.rename({4:'20220411'}, inplace=True)
    groupd = paqc1.groupby(by=['F'])
    # paqc1 = paqc1.rename({4:'20220411'}, inplace=True)
    rs3 = groupd.sum()

    return rs3


# 未使用
def recordAlist(*kw):
    a1 = {}
    rsashard = pandas.DataFrame(columns=[])
    # a1[0] = banKuai(fordate)
    for i in kw:
        a1[i] = aShards(i).set_index(['说明'])
        a1[i] = a1[i].drop(labels='股票代码', axis=1)
        a1[i] = a1[i].drop(labels='C', axis=1)
        a1[i] = a1[i].drop(labels='E', axis=1)
        # rs4 = pandas.concat([rs4,a1[i]],axis=1)
        rsashard = pandas.merge(rsashard, a1[i], how='outer', left_index=True, right_index=True)

        # rs4 = rs4.join(a1[i],how='right')
        # rsashard['forsum']  = rsashard.sum(axis=1)
        # rsashard = rsashard.sort_values(by=('forsum') ,ascending=False ,inplace=False)
        # rsashard = rsashard.drop(labels='forsum' ,axis=1)
    return rsashard


def aShardslist(*kw):
    a1 = {}
    rsashard = pandas.DataFrame(columns=[])
    # a1[0] = banKuai(fordate)
    for i in kw:
        a1[i] = aShards(i)
        a1[i] = a1[i].drop(labels='股票代码', axis=1)
        a1[i] = a1[i].drop(labels='C', axis=1)
        a1[i] = a1[i].drop(labels='E', axis=1)
        # rs4 = pandas.concat([rs4,a1[i]],axis=1)
        rsashard = pandas.merge(rsashard, a1[i], how='outer', left_index=True, right_index=True)
        # rs4 = rs4.join(a1[i],how='right')
        rsashard = rsashard.loc[(rsashard.sum(axis=1) != 0)]
        rsashard['forsum'] = rsashard.sum(axis=1)
        rsashard = rsashard.sort_values(by=('forsum'), ascending=False, inplace=False)
        rsashard = rsashard.drop(labels='forsum', axis=1)
    for i in kw:
        rsashard[i] = rsashard[i].fillna(0)
        rsashard[i] = rsashard[i] // 10000
    return rsashard


def banKuailist(kw):
    a1 = {}
    rs4 = pandas.DataFrame(columns=[])
    # a1[0] = banKuai(fordate)
    for i in kw:
        a1[i] = banKuai(i)
        # rs4 = pandas.concat([rs4,a1[i]],axis=1)
        rs4 = pandas.merge(rs4, a1[i], how='outer', left_index=True, right_index=True)
        # rs4 = rs4.join(a1[i],how='right')
        rs4 = rs4.loc[(rs4.sum(axis=1) != 0)]
    for i in kw:
        rs4[i] = rs4[i].fillna(0)
        rs4[i] = rs4[i] // 10000

    return rs4


def aShardslist(kw):
    a1 = {}
    aShardslv = pandas.DataFrame(columns=[])
    # a1[0] = banKuai(fordate)
    for i in kw:
        a1[i] = aShardslv_sa(i)
        a1[i] = a1[i].drop(labels='股票代码', axis=1)
        a1[i] = a1[i].drop(labels='C', axis=1)
        a1[i] = a1[i].drop(labels='E', axis=1)
        # rs4 = pandas.concat([rs4,a1[i]],axis=1)
        aShardslv = pandas.merge(aShardslv, a1[i], how='outer', left_index=True, right_index=True)
        # rs4 = rs4.join(a1[i],how='right')
        aShardslv = aShardslv.loc[(aShardslv.sum(axis=1) != 0)]
        aShardslv['forsum'] = aShardslv.sum(axis=1)
        aShardslv = aShardslv.sort_values(by=('forsum'), ascending=False, inplace=False)
        aShardslv = aShardslv.drop(labels='forsum', axis=1)
    for i in kw:
        aShardslv[i] = aShardslv[i].fillna(0)
        # aShardslv[i] = aShardslv[i] // 10000
    return aShardslv


def forpicture3(fordata, fordate, filepath):
    try:
        # import xlwings as xw
        font = FontProperties(
            fname=r"/home/toptrade/anaconda3/envs/selpy39/lib/python3.9/site-packages/matplotlib/mpl-data/fonts/ttf/SimHei.ttf",
            size=18)
        # plt.rcParams["font.family"] = "SimHei"
        # plt.rcParams['font.sans-serif'] = ['SimHei']
        # plt.rcParams['axes.unicode_minus'] = False
        data = fordata.values
        # fig,ax = plt.subplots(nrows=len(fordata.values),ncols=1) # 1行一列 1个图
        # axes = ax.flatten()
        # ax.get_xaxis().get_major_formatter().set_useOffset(False)
        x = fordata.columns.values[0:].tolist()  # 横坐标
        fortitle = fordata._stat_axis.values.tolist()
        # ax.set_title("合并",fontsize=18,backgroundcolor='#3c7f99',fontweight='bold',color='white',verticalalignment="baseline")
        for i in range(len(fordata.values)):
            fig, ax = plt.subplots(nrows=1, ncols=1, tight_layout=True)  # 1行一列 1个图
            # tick_spacing = 10
            # ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
            ax.get_xaxis().get_major_formatter().set_useOffset(False)

            y = data[i]  # [1:] #第一条
            # axes[i].plot(x,y[0:],'.-b',label=i, linewidth=2)
            # plt.plot(x,y[0:],'.-b',label=i, linewidth=2)
            v_bar = plt.bar(x, y, color='red')
            for bar, height in zip(v_bar, y):
                if height < 0:
                    bar.set(color='green')
            ax.set_title(fortitle[i], fontsize=18, backgroundcolor='#3c7f99', fontweight='bold', color='white',
                         verticalalignment="baseline", fontproperties=font)
            logger.info(f"{fortitle[i]}:{i}")
            # ax.set_xticklabels(rotation=75)

            # app=xw.App(visible=False,add_book=True) #后台操作，可添加book
            # bk=xw.Book(filename)
            # bk=app.books.open(fordir)
            # print(fordir)
            # sht=bk.sheets[0] #新建一个工作表
            # sht=bk.sheets['A股历史(单位万)']
            # sht.activate()
            # sht=bk.sheets.add()
            # sht.name='figureaaa' #命名
            # sht.pictures.add(fig,name='myplot',update=True,left=sht.range('L10').left, top=sht.range('L10').top, width=400, height=(200 + 15 * i))
            if not os.path.exists(filepath):
                os.makedirs(filepath)
            plt.xticks(rotation=75)
            plt.savefig(filepath + '/{}_{}-{}.png'.format(fordate, i, fortitle[i].replace('*', '')), dpi=72)
            plt.close()

            # plt.show()
        # plt.show()
        # app=xw.App(visible=False,add_book=True) #后台操作，可添加book
        # bk=xw.Book(filename)
        # bk=app.books.open(fordir)
        # print(fordir)
        # sht=bk.sheets[0] #新建一个工作表
        # sht=bk.sheets['A股历史(单位万)']
        # sht.activate()
        # sht=bk.sheets.add()
        # sht.name='figureaaa' #命名
        # sht.pictures.add(fig,name='myplot',update=True,left=sht.range('B10').left, top=sht.range('B10').top, width=400, height=200)
        # sht.pictures.add(fig,name='myplot',update=True,left=sht.range('L10').left, top=sht.range('L10').top, width=400, height=(200 + 15 * i))
        # bk.save()
        # bk.close()
    except Exception as e:
        logger.error(e)
        print(e)


def forpicture2(fordata, fordate, filepath):
    try:
        # import xlwings as xw
        font = FontProperties(
            fname=r"/home/toptrade/anaconda3/envs/selpy39/lib/python3.9/site-packages/matplotlib/mpl-data/fonts/ttf/SimHei.ttf",
            size=18)
        # plt.rcParams["font.family"] = "SimHei"
        # plt.rcParams['font.sans-serif'] = ['SimHei']
        # plt.rcParams['axes.unicode_minus'] = False
        data = fordata.values
        # fig,ax = plt.subplots(nrows=len(fordata.values),ncols=1) # 1行一列 1个图
        # axes = ax.flatten()
        # ax.get_xaxis().get_major_formatter().set_useOffset(False)
        x = fordata.columns.values[0:].tolist()  # 横坐标
        fortitle = fordata._stat_axis.values.tolist()
        # ax.set_title("合并",fontsize=18,backgroundcolor='#3c7f99',fontweight='bold',color='white',verticalalignment="baseline")
        for i in range(len(fordata.values)):
            fig, ax = plt.subplots(nrows=1, ncols=1, tight_layout=True)  # 1行一列 1个图
            # tick_spacing = 10
            # ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
            ax.get_xaxis().get_major_formatter().set_useOffset(False)

            y = data[i]  # [1:] #第一条
            # axes[i].plot(x,y[0:],'.-b',label=i, linewidth=2)
            # plt.plot(x,y[0:],'.-b',label=i, linewidth=2)
            v_bar = plt.bar(x, y, color='red')
            for bar, height in zip(v_bar, y):
                if height < 0:
                    bar.set(color='green')
            ax.set_title(fortitle[i], fontsize=18, backgroundcolor='#3c7f99', fontweight='bold', color='white',
                         verticalalignment="baseline", fontproperties=font)
            logger.info(f"{fortitle[i]}:{i}")
            # ax.set_xticklabels(rotation=75)

            # app=xw.App(visible=False,add_book=True) #后台操作，可添加book
            # bk=xw.Book(filename)
            # bk=app.books.open(fordir)
            # print(fordir)
            # sht=bk.sheets[0] #新建一个工作表
            # sht=bk.sheets['A股历史(单位万)']
            # sht.activate()
            # sht=bk.sheets.add()
            # sht.name='figureaaa' #命名
            # sht.pictures.add(fig,name='myplot',update=True,left=sht.range('L10').left, top=sht.range('L10').top, width=400, height=(200 + 15 * i))
            if not os.path.exists(filepath):
                os.makedirs(filepath)
            plt.xticks(rotation=75)
            plt.savefig(filepath + '/{}_{}-{}.png'.format(fordate, i, fortitle[i].replace('*', '')), dpi=72)
            plt.close()

            # plt.show()
        # plt.show()
        # app=xw.App(visible=False,add_book=True) #后台操作，可添加book
        # bk=xw.Book(filename)
        # bk=app.books.open(fordir)
        # print(fordir)
        # sht=bk.sheets[0] #新建一个工作表
        # sht=bk.sheets['A股历史(单位万)']
        # sht.activate()
        # sht=bk.sheets.add()
        # sht.name='figureaaa' #命名
        # sht.pictures.add(fig,name='myplot',update=True,left=sht.range('B10').left, top=sht.range('B10').top, width=400, height=200)
        # sht.pictures.add(fig,name='myplot',update=True,left=sht.range('L10').left, top=sht.range('L10').top, width=400, height=(200 + 15 * i))
        # bk.save()
        # bk.close()
    except Exception as e:
        logger.error(e)
        print(e)


def get_benrate(series):
    changenum = series['changenum']
    totmktcap = series['totmktcap']
    if totmktcap == 0:
        return 0
    else:
        return changenum / totmktcap


def get_marketvalue_rate(series):
    changenum = series['changenum']
    totmktcap = series['totmktcap']
    AVGPRICE = series['AVGPRICE']
    if totmktcap == 0:
        return 0
    else:
        return (changenum * AVGPRICE) / (totmktcap * 10000)


def get_industry_rate(series):
    changenum_AVGPRICE = series['changenum_AVGPRICE']
    totmktcap = series['totmktcap']
    if totmktcap == 0:
        return 0
    else:
        return changenum_AVGPRICE / totmktcap


def stock_ktcap_rate(series):
    df = series.str.split(",", expand=True)
    df.columns = ["sum_changenum", "AVGPRICE", "totmktcap"]
    df['sum_changenum'] = df['sum_changenum'].astype(np.float64)
    df['AVGPRICE'] = df['AVGPRICE'].astype(np.float64)
    df['totmktcap'] = df['totmktcap'].astype(np.float64)
    return round(df["sum_changenum"] / df["totmktcap"] * 100 if df['totmktcap'].sum() != 0 else 0.0, 2)


def stock_ktcap_day_rate(x):
    df = x.str.split(",", expand=True)
    if df.shape[1] == 3:
        df.columns = ["sum_changenum", "AVGPRICE", "totmktcap"]
        df['sum_changenum'] = df['sum_changenum'].astype(np.float64)
        df['AVGPRICE'] = df['AVGPRICE'].astype(np.float64)
        df['totmktcap'] = df['totmktcap'].astype(np.float64)
        cap = df['sum_changenum'].sum() / df['totmktcap'].iloc[0,] * 100 if df['totmktcap'].sum() != 0 else 0.0
        return round(cap, 2)


@logger.catch
def history_A_stock_changenum(tradedate_list):
    """
    股票近10日买入量(万股)
    """
    try:
        trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
        trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
        for date in tradedate_list:
            # sql = f'''select stockcode,stockname,industryname,TOTALSHARE,CIRCSKAMT,ROUND(holdnum /100 / CIRCSKAMT , 2),totalnu from (select stockcode,case substr(stockname, 1, 2) when 'DR' then( select distinct (stockname) from T_O_BrokerAllPosition f where f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1 ) when 'XD' then ( select distinct (stockname)from T_O_BrokerAllPosition f where f.stockname not like 'DR%'and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,ROUND(g.TOTALSHARE / 10000,2) TOTALSHARE,ROUND(g.CIRCSKAMT / 10000,2) CIRCSKAMT, ROUND(sum(t.holdnum) /10000 , 2) holdnum,sum(changenum) totalnu from T_O_BrokerAllPosition t ,TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO s where t.tradeday = to_date({date}, 'yyyy/mm/dd') and t.stockcode = s.symbol and g.compcode = s.compcode and  g.ENTRYDATE = ( select max(g.ENTRYDATE) from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO f where g.compcode = f.compcode and f.symbol = s.symbol) and  g.begindate = ( select max(g.begindate) from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO f where g.compcode = f.compcode and f.symbol = s.symbol) GROUP BY stockcode, stockname, industryname,ROUND(g.TOTALSHARE / 10000,2),ROUND(g.CIRCSKAMT / 10000,2))'''
            sql = f'''select t.stockcode, case substr(stockname, 1, 2) when 'DR' then (select distinct (stockname) from T_O_BrokerAllPosition f where f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct (stockname) from T_O_BrokerAllPosition f where f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname, industryname, ROUND(g.TOTALSHARE / 10000, 2) TOTALSHARE, ROUND(g.CIRCSKAMT / 10000, 2) CIRCSKAMT, ROUND(ft.holdnum/100 / g.CIRCSKAMT , 2) holdnum, sum(changenum) totalnu from T_O_BrokerAllPosition t, TQ_SK_SHARESTRUCHG g, TQ_SK_BASICINFO s, (select a.stockcode, sum(a.holdnum) holdnum from T_O_BrokerAllPosition a where a.tradeday = to_date({tradedate_list[-1]}, 'yyyy/mm/dd') GROUP BY a.stockcode) ft  where t.tradeday = to_date({date}, 'yyyy/mm/dd') and t.stockcode = s.symbol and g.compcode = s.compcode and t.stockcode = ft.stockcode and g.ENTRYDATE = (select max(g.ENTRYDATE) from TQ_SK_SHARESTRUCHG g, TQ_SK_BASICINFO f where g.compcode = f.compcode and f.symbol = s.symbol) and g.begindate = (select max(g.begindate) from TQ_SK_SHARESTRUCHG g, TQ_SK_BASICINFO f where g.compcode = f.compcode and f.symbol = s.symbol) GROUP BY t.stockcode, stockname, industryname, ROUND(ft.holdnum/100 / g.CIRCSKAMT , 2), ROUND(g.TOTALSHARE / 10000, 2), ROUND(g.CIRCSKAMT / 10000, 2)'''
            df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
            dflist = []
            for chunk in df_changenum_list:
                dflist.append(chunk)
            df_changenum = pd.concat(dflist)
            df_changenum.columns = ["stockcode", "stockname", "industryname", "TOTALSHARE", "CIRCSKAMT", "持仓流通占比", date]
            df_changenum.set_index(["stockcode", 'stockname', "industryname", "TOTALSHARE", "CIRCSKAMT", "持仓流通占比"],
                                   drop=True,
                                   append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        trade_df.sort_values(by=tradedate_list, ascending=False, inplace=True)
        trade_df['近三天'] = trade_df[tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近五天'] = trade_df[tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近十天'] = trade_df[tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
        # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
        trade_df = trade_df.apply(lambda x: (x / 10000).round())
        trade_df.fillna(0, inplace=True)
        return trade_df
    except:
        return pd.DataFrame()


@logger.catch
def history_stock_ktcap(tradedate_list):
    """
    股票相对流通股近10日变动率(%)
    """
    try:
        trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
        trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
        for date in tradedate_list:
            sql = f"""SELECT t.stockcode,t.stockname,t.industryname,t.sum_changenum ||','|| q.CIRCSKAMT ||',' || q.CIRCSKAMT * 10000 AS text FROM (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  FROM T_O_BrokerAllPosition t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') GROUP BY stockcode,stockname,industryname) t, TQ_SK_SHARESTRUCHG q, TQ_SK_BASICINFO s WHERE q.compcode =  s.compcode  AND t.stockcode=s.SYMBOL and  q.ENTRYDATE = ( select max(g.ENTRYDATE) from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO l where g.compcode = l.compcode and l.symbol = s.symbol)  and  q.begindate = ( select max(g.begindate) from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO l where g.compcode = l.compcode and l.symbol = s.symbol) and t.sum_changenum != 0"""
            df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
            dflist = []
            for chunk in df_changenum_list:
                dflist.append(chunk)
            df_changenum = pd.concat(dflist)
            df_changenum.columns = ["stockcode", "stockname", "industryname", date]
            df_changenum.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
        trade_df.fillna("0,1,1", inplace=True)

        df = trade_df.apply(stock_ktcap_rate)
        trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        df['近三天'] = trade_df.loc[:, tradedate_list[-3:]].apply(stock_ktcap_day_rate, axis=1)
        df['近五天'] = trade_df.loc[:, tradedate_list[-5:]].apply(stock_ktcap_day_rate, axis=1)
        df['近十天'] = trade_df.loc[:, tradedate_list[0:10]].apply(stock_ktcap_day_rate, axis=1)
        df.fillna(0, inplace=True)
        df = df.loc[(df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
        return df
    except:
        return pd.DataFrame()


@logger.catch
def history_A_stock_industry(tradedate_list):
    """
    板块近10日买入额(万)
    """
    try:
        trade_df = pd.DataFrame(columns=["industrycode", "industryname"])
        trade_df.set_index(["industrycode", 'industryname'], drop=True, append=False, inplace=True)
        for date in tradedate_list:
            industry_totmktcap_sql = f'''
                SELECT t.industrycode,t.industryname,ROUND(SUM(t.changenum*q.AVGPRICE)) AS sum_mul_chclose FROM (
        	    SELECT industrycode,industryname,stockcode,stockname,changenum FROM T_O_BrokerAllPosition 
                WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s 
                WHERE q.secode =  s.secode  AND t.stockcode=s.SYMBOL  AND q.tradedate={date} GROUP BY t.industryname,t.industrycode'''
            df_industry_list = pd.read_sql(industry_totmktcap_sql, engine, chunksize=10000)
            dflist = []
            for chunk in df_industry_list:
                dflist.append(chunk)
            df_industry = pd.concat(dflist)
            df_industry.columns = ["industrycode", "industryname", date]
            df_industry.set_index(["industrycode", 'industryname'], drop=True, append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_industry, how='outer', left_index=True, right_index=True)
            if ('0', '0') in trade_df.index.tolist():
                trade_df.drop(index=('0', '0'), inplace=True)
        trade_df['近三天'] = trade_df[tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近五天'] = trade_df[tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近十天'] = trade_df[tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
        trade_df = trade_df.apply(lambda x: (x / 10000).round())
        trade_df.fillna(0, inplace=True)
        return trade_df
    except:
        return pd.DataFrame()


def history_stock_rate(s):
    df = s.str.split(",", expand=True)
    df.columns = ["sum_mul_chclose", "sum_totmktcap"]
    df['sum_mul_chclose'] = df['sum_mul_chclose'].astype(np.float64)
    df['sum_totmktcap'] = df['sum_totmktcap'].astype(np.float64)
    return round(df['sum_mul_chclose'] / df['sum_totmktcap'] * 100, 2)


def history_stock_day_rate(x):
    df = x.str.split(",", expand=True)
    df.columns = ["sum_mul_chclose", "sum_totmktcap"]
    df['sum_mul_chclose'] = df['sum_mul_chclose'].astype(np.float64)
    df['sum_totmktcap'] = df['sum_totmktcap'].astype(np.float64)
    ktcap = df['sum_mul_chclose'].sum() / df['sum_totmktcap'].sum() * 100 if df['sum_totmktcap'].sum() != 0 else 0
    return round(ktcap, 2)


@logger.catch
def history_stock_industry(tradedate_list):
    """
    板块相较于申万板块近10日变动率(%)
    """
    try:
        trade_df = pd.DataFrame(columns=["industrycode", "industryname"])
        trade_df.set_index(["industrycode", 'industryname'], drop=True, append=False, inplace=True)
        for date in tradedate_list:
            industry_totmktcap_sql = f"""
                SELECT tt.industrycode,tt.industryname,tt.sum_mul_chclose ||','|| qq.sum_totmktcap FROM (
                SELECT t.industrycode,t.industryname,SUM(t.changenum*q.AVGPRICE) AS sum_mul_chclose FROM (
                SELECT industrycode,industryname,stockcode,stockname,changenum FROM T_O_BrokerAllPosition WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) t, 
                TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s WHERE q.secode =  s.secode  AND t.stockcode=s.SYMBOL AND q.tradedate={date} GROUP BY t.industryname,t.industrycode
                ) tt,
                (
                SELECT t.industrycode,t.industryname,SUM(q.totmktcap) AS sum_totmktcap FROM TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s,T_O_BrokerAllPosition t 
                WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date} GROUP BY t.industryname,t.industrycode
                ) qq 
                WHERE tt.industrycode = qq.industrycode"""
            df_industry_list = pd.read_sql(industry_totmktcap_sql, engine, chunksize=10000)
            dflist = []
            for chunk in df_industry_list:
                dflist.append(chunk)
            df_industry = pd.concat(dflist)
            df_industry.columns = ["industrycode", "industryname", date]
            df_industry.set_index(["industrycode", 'industryname'], drop=True, append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_industry, how='outer', left_index=True, right_index=True)
            if ('0', '0') in trade_df.index.tolist():
                trade_df.drop(index=('0', '0'), inplace=True)
        trade_df.fillna("0,1", inplace=True)
        df = trade_df.apply(history_stock_rate)
        df['近三天'] = trade_df.loc[:, tradedate_list[-3:]].apply(history_stock_day_rate, axis=1)
        df['近五天'] = trade_df.loc[:, tradedate_list[-5:]].apply(history_stock_day_rate, axis=1)
        df['近十天'] = trade_df.loc[:, tradedate_list[0:10]].apply(history_stock_day_rate, axis=1)
        df.fillna(0, inplace=True)
        df = df.loc[(df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
        return df
    except:
        return pd.DataFrame()


@logger.catch
def history_stock_broker(tradedate_list):
    """
    经纪商近10日买入额(万)
    """
    try:
        trade_df = pd.DataFrame(columns=["brokercode", "brokername"])
        trade_df.set_index(["brokercode", 'brokername'], drop=True, append=False, inplace=True)
        for date in tradedate_list:
            broker_sql = f'''
                SELECT t.brokercode,t.brokername,ROUND(SUM(q.AVGPRICE*t.changenum)) AS sum_mul_chclose
                FROM    T_O_BrokerAllPosition t,TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s 
                WHERE   q.secode =  s.secode AND t.stockcode=s.SYMBOL AND q.tradedate ={date} AND tradeday = to_date({date}, 'yyyy/mm/dd') GROUP BY t.brokercode,t.brokername
                '''
            df_broker_list = pd.read_sql(broker_sql, engine, chunksize=10000)
            dflist = []
            for chunk in df_broker_list:
                dflist.append(chunk)
            df_broker = pd.concat(dflist)
            df_broker.columns = ["brokercode", "brokername", date]
            df_broker.set_index(["brokercode", 'brokername'], drop=True, append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_broker, how='outer', left_index=True, right_index=True)
        trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        trade_df.dropna(how='all', inplace=True)
        trade_df.sort_values(by=tradedate_list, ascending=False, inplace=True)
        trade_df = trade_df.apply(lambda x: (x // 10000))
        trade_df['近三天'] = trade_df[tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近五天'] = trade_df[tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近十天'] = trade_df[tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
        return trade_df
    except:
        return pd.DataFrame()


@logger.catch
def create_dir(sftp, remoteDir):
    try:
        if stat.S_ISDIR(sftp.stat(remoteDir).st_mode):
            pass
    except Exception as e:
        sftp.mkdir(remoteDir)
        print("在远程sftp上创建目录：{}".format(remoteDir))


@logger.catch
def sftp_upload(sftp, localDir, remoteDir):
    """
    sftp文件上传
    """
    if os.path.isdir(localDir):  # 判断本地localDir是否为目录
        try:
            if stat.S_ISDIR(sftp.stat(remoteDir).st_mode):
                pass
        except Exception as e:
            sftp.mkdir(remoteDir)
            print("在远程sftp上创建目录：{}".format(remoteDir))
        for file in os.listdir(localDir):
            remoteDirTmp = os.path.join(remoteDir, file)
            localDirTmp = os.path.join(localDir, file)
            sftp_upload(sftp, localDirTmp, remoteDirTmp)
    else:
        print("upload file:", localDir)
        logger.info("upload file:" + localDir)
        try:
            sftp.put(localDir, remoteDir)
        except Exception as e:
            print('upload error:', e)


@logger.catch
def vba_sftp_upload_xlsm(bio, remote_path, fordate):
    """
    上传excel文件，流方式
    """
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname='192.168.101.211', username='toptrade', password='toptrade')
        stdin, stdout, stderr = ssh_client.exec_command(
            "echo 'toptrade' | sudo -S chmod 777 /home/guest/003-数据/001-经纪商数据 -R", timeout=300)
        out = stdout.readlines()
        ftp_client = ssh_client.open_sftp()
        xlsm = open('./xlsm' + '/A股额度' + fordate + '.xlsm', "wb")
        with ftp_client.open(remote_path + '/A股额度' + fordate + '.xlsm', "w") as f:
            bio.seek(0)
            xlsm_byes = bio.read()
            xlsm.write(xlsm_byes)
            f.write(xlsm_byes)
            logger.info('/A股额度' + fordate + '.xlsm')
    except Exception as e:
        logger.error(e)


@logger.catch
def sftp_upload_img(local_path, remote_path, fordate, despath):
    """
    上传图片
    """
    try:
        host = '192.168.101.211'  # sftp主机
        port = 22  # 端口
        username = 'toptrade'  # sftp用户名
        password = 'toptrade'  # 密码
        localDir = local_path  # 本地文件或目录
        remoteDir = f'{remote_path}/{despath}/'  #
        sf = paramiko.Transport((host, port))
        sf.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(sf)
        sftp_upload(sftp, localDir, remoteDir)
        sf.close()
    except Exception as e:
        logger.error(e)


def get_tradedate_list_month():
    """
    获取本月交易日
    """
    tradedate_sql = "SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_BrokerAllPosition WHERE to_char(tradeday,'mm')=to_char(sysdate,'mm') AND to_char(tradeday,'yyyy')=to_char(sysdate,'yyyy') ORDER BY TRADEDAY desc) tr"
    df_tradedate = pd.read_sql(tradedate_sql, engine)
    tradedate_list = sorted(df_tradedate["tradeday"].tolist())
    if len(tradedate_list) == 0:
        tradedate_sql = "SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_BrokerAllPosition ORDER BY TRADEDAY desc) tr WHERE rownum<=10"
        df_tradedate = pd.read_sql(tradedate_sql, engine)
        tradedate_list = sorted(df_tradedate["tradeday"].tolist())
    return tradedate_list


@logger.catch
def get_tradedate_list_rownum(tradedate: str = None) -> List[str]:
    """
    获取10个交易日
    """
    try:
        tradedate_sql = "SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_BrokerAllPosition ORDER BY TRADEDAY desc) tr WHERE rownum<=10"
        if tradedate:
            tradedate_sql = f"SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_BrokerAllPosition ORDER BY TRADEDAY desc) tr WHERE tradeday <= to_date({tradedate}, 'yyyy/mm/dd') AND rownum<=10"
        df_tradedate = pd.read_sql(tradedate_sql, engine)
        tradedate_list = sorted(df_tradedate["tradeday"].tolist())
        return tradedate_list
    except Exception as e:
        logger.error(e)


def highlight_background(val):
    """
    设置背景颜色
    """
    color = "#F9FF51"
    if val > 0:
        color = "#FF350C"
    elif val < 0:
        color = "#24FF1C"

    return 'background-color: %s' % color


@logger.catch
def history_A_stock_ktcap(tradedate_list):
    """
    股票近10日买入额(万)
    """
    try:
        trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
        trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
        # and t.changenum !=0
        for date in tradedate_list:
            sql = f'''select a1.stockcode,a1.stockname,a1.industryname,ROUND(a2.TOTMKTCAP / 10000),ROUND(a2.NEGOTIABLEMV / 10000),a2.marketvalue,ROUND(a2.marketvalue / ROUND(a2.NEGOTIABLEMV / 10000) * 100,2),a1.mul_totmktcap  from (SELECT t.stockcode,t.stockname,industryname, ROUND(t.sum_changenum * q.AVGPRICE) AS mul_totmktcap FROM  (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) when 'TC' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'TC%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  FROM T_O_BrokerAllPosition t WHERE tradeday = to_date({date}, 'yyyy/mm/dd')  GROUP BY stockcode,stockname,industryname) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s WHERE q.secode =  s.secode  AND  t.stockcode=s.SYMBOL AND q.tradedate={date} ) a1 , (SELECT t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV,sum(t.marketvalue) /100000000 marketvalue from TQ_QT_SKDAILYPRICE l, TQ_SK_BASICINFO s, T_O_BrokerAllPosition t  where t.tradeday = to_date({tradedate_list[-1]}, 'yyyy/mm/dd')  and l.secode =  s.secode AND t.stockcode=s.SYMBOL AND l.tradedate={tradedate_list[-1]} GROUP BY t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV) a2 where a1.stockcode = a2.stockcode'''
            df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
            dflist = []
            for chunk in df_changenum_list:
                dflist.append(chunk)
            df_changenum = pd.concat(dflist)
            df_changenum.columns = ["stockcode", "stockname", "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)",
                                    date]
            df_changenum.set_index(
                ["stockcode", 'stockname', "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)"], drop=True,
                append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        trade_df.sort_values(by=tradedate_list, ascending=False, inplace=True)
        trade_df['近三天'] = trade_df[tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近五天'] = trade_df[tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近十天'] = trade_df[tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
        # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
        trade_df = trade_df.apply(lambda x: (x / 10000).round()).sort_index()
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
        # 获取前5
        trade_df_for5 = trade_df[trade_df['近五天'] > 0].copy()
        trade_df_for5.sort_values(by='industryname', ascending=False, inplace=True)

        trade_df_for5 = trade_df_for5.groupby(['industryname']).head(5)
        print('---------------4')

        # 连续5天买入
        trade_df_for5buy = trade_df[(trade_df[tradedate_list[-1]] > 0) & (trade_df[tradedate_list[-2]] > 0) & (
                trade_df[tradedate_list[-3]] > 0) & (trade_df[tradedate_list[-4]] > 0) & (
                                            trade_df[tradedate_list[-5]] > 0)].copy()
        return trade_df, trade_df_for5, trade_df_for5buy
    except:
        return pd.DataFrame()


@logger.catch
def history_A_stock_ktcap_chicang(tradedate_list):
    """
    持仓股票近10日买入额(万)
    """
    try:
        trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
        trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
        # and t.changenum !=0
        for date in tradedate_list:
            sql = f'''select a1.stockcode,a1.stockname,a1.industryname,ROUND(a2.TOTMKTCAP / 10000),ROUND(a2.NEGOTIABLEMV / 10000),a2.marketvalue,ROUND(a2.marketvalue / ROUND(a2.NEGOTIABLEMV / 10000) * 100,2),a1.mul_totmktcap  from (SELECT t.stockcode,t.stockname,industryname, ROUND(t.sum_changenum * q.AVGPRICE) AS mul_totmktcap FROM  (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) when 'TC' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'TC%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  FROM T_O_BrokerAllPosition t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode in (select substr(stockcode, 3, 6) from T_O_FORUSSTOCK t where t.forstat = '持仓' and t.insertday = to_date({tradedate_list[- 1]}, 'yyyy/mm/dd'))  GROUP BY stockcode,stockname,industryname) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s WHERE q.secode =  s.secode  AND  t.stockcode=s.SYMBOL AND q.tradedate={date} ) a1 , (SELECT t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV,sum(t.marketvalue) /100000000 marketvalue from TQ_QT_SKDAILYPRICE l, TQ_SK_BASICINFO s, T_O_BrokerAllPosition t  where t.tradeday = to_date({tradedate_list[-1]}, 'yyyy/mm/dd')  and l.secode =  s.secode AND t.stockcode=s.SYMBOL AND l.tradedate={tradedate_list[-1]} GROUP BY t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV) a2 where a1.stockcode = a2.stockcode'''
            df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
            dflist = []
            for chunk in df_changenum_list:
                dflist.append(chunk)
            df_changenum = pd.concat(dflist)
            df_changenum.columns = ["stockcode", "stockname", "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)",
                                    date]
            df_changenum.set_index(
                ["stockcode", 'stockname', "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)"], drop=True,
                append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        trade_df.sort_values(by=tradedate_list, ascending=False, inplace=True)
        trade_df['近三天'] = trade_df[tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近五天'] = trade_df[tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近十天'] = trade_df[tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
        # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
        trade_df = trade_df.apply(lambda x: (x / 10000).round()).sort_index()
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
        return trade_df
    except:
        return pd.DataFrame()


@logger.catch
def history_A_stock_ktcap_bankuai(tradedate_list):
    """
    板块股票近10日买入额(万)
    """
    try:
        trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
        trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
        # and t.changenum !=0
        for date in tradedate_list:
            sql = f'''select a1.stockcode,a1.stockname,a1.industryname,ROUND(a2.TOTMKTCAP / 10000),ROUND(a2.NEGOTIABLEMV / 10000),a2.marketvalue,ROUND(a2.marketvalue / ROUND(a2.NEGOTIABLEMV / 10000) * 100,2),a1.mul_totmktcap  from (SELECT t.stockcode,t.stockname,industryname, ROUND(t.sum_changenum * q.AVGPRICE) AS mul_totmktcap FROM  (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) when 'TC' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'TC%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  FROM T_O_BrokerAllPosition t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode in (select substr(stockcode, 3, 6) from T_O_FORUSSTOCK t where t.forstat = '预测' and t.insertday = to_date({tradedate_list[- 1]}, 'yyyy/mm/dd'))  GROUP BY stockcode,stockname,industryname) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s WHERE q.secode =  s.secode  AND  t.stockcode=s.SYMBOL AND q.tradedate={date} ) a1 , (SELECT t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV,sum(t.marketvalue) /100000000 marketvalue from TQ_QT_SKDAILYPRICE l, TQ_SK_BASICINFO s, T_O_BrokerAllPosition t  where t.tradeday = to_date({tradedate_list[-1]}, 'yyyy/mm/dd')  and l.secode =  s.secode AND t.stockcode=s.SYMBOL AND l.tradedate={tradedate_list[-1]} GROUP BY t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV) a2 where a1.stockcode = a2.stockcode'''
            df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
            dflist = []
            for chunk in df_changenum_list:
                dflist.append(chunk)
            df_changenum = pd.concat(dflist)
            df_changenum.columns = ["stockcode", "stockname", "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)",
                                    date]
            df_changenum.set_index(
                ["stockcode", 'stockname', "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)"], drop=True,
                append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        trade_df.sort_values(by=tradedate_list, ascending=False, inplace=True)
        trade_df['近三天'] = trade_df[tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近五天'] = trade_df[tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近十天'] = trade_df[tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
        # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
        trade_df = trade_df.apply(lambda x: (x / 10000).round()).sort_index()
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
        return trade_df
    except:
        return pd.DataFrame()


@logger.catch
def history_A_stock_ktcap_xiaopiao(tradedate_list):
    """
    观察股票近10日买入额(万)
    """
    try:
        trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
        trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
        # and t.changenum !=0
        for date in tradedate_list:
            sql = f'''select a1.stockcode,a1.stockname,a1.industryname,ROUND(a2.TOTMKTCAP / 10000),ROUND(a2.NEGOTIABLEMV / 10000),a2.marketvalue,ROUND(a2.marketvalue / ROUND(a2.NEGOTIABLEMV / 10000) * 100,2),a1.mul_totmktcap  from (SELECT t.stockcode,t.stockname,industryname, ROUND(t.sum_changenum * q.AVGPRICE) AS mul_totmktcap FROM  (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) when 'TC' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'TC%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  FROM T_O_BrokerAllPosition t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode in (select substr(stockcode, 3, 6) from T_O_FORUSSTOCK t where t.forstat = '观察' and t.insertday = to_date({tradedate_list[- 1]}, 'yyyy/mm/dd'))  GROUP BY stockcode,stockname,industryname) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s WHERE q.secode =  s.secode  AND  t.stockcode=s.SYMBOL AND q.tradedate={date} ) a1 , (SELECT t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV,sum(t.marketvalue) /100000000 marketvalue from TQ_QT_SKDAILYPRICE l, TQ_SK_BASICINFO s, T_O_BrokerAllPosition t  where t.tradeday = to_date({tradedate_list[-1]}, 'yyyy/mm/dd')  and l.secode =  s.secode AND t.stockcode=s.SYMBOL AND l.tradedate={tradedate_list[-1]} GROUP BY t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV) a2 where a1.stockcode = a2.stockcode'''
            df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
            dflist = []
            for chunk in df_changenum_list:
                dflist.append(chunk)
            df_changenum = pd.concat(dflist)
            df_changenum.columns = ["stockcode", "stockname", "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)",
                                    date]
            df_changenum.set_index(
                ["stockcode", 'stockname', "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)"], drop=True,
                append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        trade_df.sort_values(by=tradedate_list, ascending=False, inplace=True)
        trade_df['近三天'] = trade_df[tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近五天'] = trade_df[tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
        trade_df['近十天'] = trade_df[tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
        # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
        trade_df = trade_df.apply(lambda x: (x / 10000).round()).sort_index()
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
        return trade_df
    except:
        return pd.DataFrame()


# 当日买入为前3日两倍以上（万股）
def history_A_stock_changenum_for3(trade_df, tradedate_list):
    try:
        trade_df = trade_df.loc[
            (trade_df.loc[:, tradedate_list[-4:-1]].sum(axis=1) < trade_df.loc[:, tradedate_list[-1:]].sum(axis=1) / 2)]
        trade_df = trade_df.loc[
            trade_df.loc[:, tradedate_list[-4:-1]].sum(axis=1) > 0]
        return trade_df
    except:
        return pd.DataFrame()


# 买卖前10
@logger.catch
def history_A_stock_top(tradedate_list):
    """
    股票至今十大买入卖出(万)
    """
    try:
        trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
        trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
        for date in tradedate_list:
            sql = f'''select tag,stockcode,stockname,industryname,total from (select 'buy' tag,to_char(t.tradeday , 'YYYY-mm-dd') dd,stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1)  else stockname end stockname,industryname, row_number() OVER(PARTITION BY to_char(t.tradeday , 'YYYY-mm-dd') ORDER BY sum(changenum)*q.AVGPRICE desc) rn ,sum(changenum)*q.AVGPRICE total from T_O_BrokerAllPosition t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s where q.secode =  s.secode and s.symbol = t.stockcode and q.tradedate = to_char(t.tradeday, 'YYYYmmdd')and  tradeday = to_date({date}, 'yyyy/mm/dd')   group by stockcode,stockname,industryname,q.AVGPRICE,to_char(t.tradeday , 'YYYY-mm-dd')) where rn < 11
                union
                select tag,stockcode,stockname,industryname,total from (select 'sell' tag,to_char(t.tradeday , 'YYYY-mm-dd') dd,stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname, row_number() OVER(PARTITION BY to_char(t.tradeday , 'YYYY-mm-dd') ORDER BY sum(changenum)*q.AVGPRICE ) rn ,sum(changenum)*q.AVGPRICE total from T_O_BrokerAllPosition t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s where q.secode =  s.secode and s.symbol = t.stockcode and q.tradedate = to_char(t.tradeday, 'YYYYmmdd')and  tradeday = to_date({date}, 'yyyy/mm/dd')   group by stockcode,stockname,industryname,q.AVGPRICE,to_char(t.tradeday , 'YYYY-mm-dd')) where rn < 11'''
            # sql = f'''select  stockcode,stockname,sum(changenum) totalnu from T_O_BrokerAllPosition t where  t.tradeday = to_date({date}, 'yyyy/mm/dd') GROUP BY stockcode,stockname '''
            df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
            dflist = []
            for chunk in df_changenum_list:
                dflist.append(chunk)
            df_changenum = pd.concat(dflist)
            df_changenum.columns = ["tag", "stockcode", "stockname", "industryname", date]
            df_changenum.drop(columns=["tag"], inplace=True)
            df_changenum.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
            trade_df = trade_df[~trade_df.index.duplicated()]
        trade_df.fillna(0, inplace=True)
        # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        trade_df["近三天"] = trade_df.loc[:, tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
        trade_df["近五天"] = trade_df.loc[:, tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
        trade_df["近十天"] = trade_df.loc[:, tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
        trade_df = trade_df.apply(lambda x: (x // 10000))
        trade_df.sort_values(by=tradedate_list, ascending=False, inplace=True)
        return trade_df
    except:
        return pd.DataFrame()


def history_A_stock_rate(s):
    df = s.str.split(",", expand=True)
    df.columns = ["holdnum", "changenum"]
    df['holdnum'] = df['holdnum'].astype(np.float64)
    df['changenum'] = df['changenum'].astype(np.float64)
    return round(df['changenum'] / (df['holdnum'] - df['changenum']) * 100, 2)


def history_A_stock_day_rate(x):
    df = x.str.split(",", expand=True)
    df.columns = ["holdnum", "changenum"]
    df['holdnum'] = df['holdnum'].astype(np.float64)
    df['changenum'] = df['changenum'].astype(np.float64)
    sto = df['changenum'].sum() / (df['holdnum'].iloc[-1] - df['changenum'].sum()) * 100 if df['holdnum'].iloc[-1] - df[
        'changenum'].sum() != 0 else 0
    return round(sto, 2)


# 股票持仓变化率（%）
@logger.catch
def history_A_stock_lv(tradedate_list: List[str]) -> pd.DataFrame:
    """
    股票持仓变化率(%)
    """
    try:
        trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
        trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
        for date in tradedate_list:
            sql = f"""select  stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,sum(holdnum) ||','|| sum(changenum) AS text
                         from T_O_BrokerAllPosition t where   tradeday = to_date({date}, 'yyyy/mm/dd')   group by stockcode,stockname,industryname"""
            df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
            dflist = []
            for chunk in df_changenum_list:
                dflist.append(chunk)
            df_changenum = pd.concat(dflist)
            df_changenum.columns = ["stockcode", "stockname", "industryname", date]
            df_changenum.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
        trade_df.fillna("1,0", inplace=True)
        # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
        # print(trade_df)
        df = trade_df.apply(history_A_stock_rate)
        df['近三天'] = trade_df.loc[:, tradedate_list[-3:]].apply(history_A_stock_day_rate, axis=1)
        df['近五天'] = trade_df.loc[:, tradedate_list[-5:]].apply(history_A_stock_day_rate, axis=1)
        df['近十天'] = trade_df.loc[:, tradedate_list[0:10]].apply(history_A_stock_day_rate, axis=1)
        df.fillna(0, inplace=True)
        df = df.loc[(df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
        return df
    except Exception as e:
        return pd.DataFrame()


def history_stock_holdnum_ktcap(tradedate_list):
    """
    股票持仓相对流通股近10日变动率(%)
    """
    try:
        trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
        trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
        for date in tradedate_list:
            sql = f"""SELECT t.stockcode,t.stockname,t.industryname,t.sum_changenum ||','|| q.CIRCSKAMT ||',' || q.CIRCSKAMT * 10000 AS text FROM (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(holdnum) AS sum_changenum  FROM T_O_BrokerAllPosition t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') GROUP BY stockcode,stockname,industryname) t, TQ_SK_SHARESTRUCHG q, TQ_SK_BASICINFO s WHERE q.compcode =  s.compcode  AND t.stockcode=s.SYMBOL and  q.ENTRYDATE = ( select max(g.ENTRYDATE) from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO l where g.compcode = l.compcode and l.symbol = s.symbol)  and  q.begindate = ( select max(g.begindate) from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO l where g.compcode = l.compcode and l.symbol = s.symbol) and t.sum_changenum != 0"""
            df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
            dflist = []
            for chunk in df_changenum_list:
                dflist.append(chunk)
            df_changenum = pd.concat(dflist)
            df_changenum.columns = ["stockcode", "stockname", "industryname", date]
            df_changenum.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
        trade_df.fillna("0,1,1", inplace=True)

        df = trade_df.apply(stock_ktcap_rate)
        trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
        df.fillna(0, inplace=True)
        df = df.loc[(df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
        return df
    except:
        return pd.DataFrame()


def history_stock_MARKETVALUE(tradedate_list):
    """
    经纪商近10日持仓持仓市值(万)
    """
    try:
        tradedate_list = sorted(tradedate_list, reverse=True)
        tradedate_list = tuple(tradedate_list)
        sql = f"""select *  from (select to_char(t.tradeday,'yyyymmdd' ) tradeday,  t.brokername,  round(sum(t.MARKETVALUE) / 10000) changenum  from T_O_BrokerAllPosition t  where t.tradeday >= to_date({tradedate_list[-1]}, 'yyyy/mm/dd')  and t.changenum != 0  group by t.tradeday,  t.brokername ) PIVOT(max(changenum)  FOR tradeday IN {tradedate_list})"""
        df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
        dflist = []
        for chunk in df_changenum_list:
            dflist.append(chunk)
        df_changenum = pd.concat(dflist)
        df_changenumcolumns = ["brokername"]
        label = df_changenumcolumns + [i[-4:] for i in tradedate_list]
        df = pd.DataFrame(df_changenum)
        df.columns = label
        df.fillna(0, inplace=True)
        df.set_index(["brokername"], drop=True, append=False, inplace=True)
        return df
    except:
        return pd.DataFrame()


def drop_duplicates_stockcode(df):
    df = df.copy()
    index_names = df.index.names
    df.reset_index(inplace=True)
    df.drop_duplicates(subset='stockcode', keep='last',
                       inplace=True)
    df.set_index(index_names, drop=True, inplace=True)
    return df


@logger.catch
def run_img_excel_sftp(tradedate: str = None) -> None:
    global global_data_res
    try:
        fordate = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
        tradedate_list = get_tradedate_list_rownum(tradedate)
        if len(tradedate_list) > 0:
            fordate = tradedate_list[-1]
        if len(tradedate_list) == 0:
            raise Exception("tradedate not exist")
        # s2 = banKuailist(tradedate_list)
        # s3 = aShardslist(tradedate_list)
        bio = BytesIO()
        with ThreadPoolExecutor(max_workers=20) as pool:
            future1 = pool.submit(history_A_stock_top, tradedate_list)
            future2 = pool.submit(history_A_stock_changenum, tradedate_list)
            # future3 = pool.submit(history_A_stock_changenum_for3, tradedate_list)
            future4 = pool.submit(history_A_stock_ktcap, tradedate_list)
            future5 = pool.submit(history_A_stock_industry, tradedate_list)
            future6 = pool.submit(history_stock_broker, tradedate_list)
            future7 = pool.submit(history_stock_ktcap, tradedate_list)
            future8 = pool.submit(history_stock_industry, tradedate_list)
            future9 = pool.submit(history_A_stock_lv, tradedate_list)
            future10 = pool.submit(banKuailist, tradedate_list)
            future11 = pool.submit(aShardslist, tradedate_list)
            future12 = pool.submit(stock_header_description, tradedate_list)
            future13 = pool.submit(history_stock_holdnum_ktcap, tradedate_list)
            future14 = pool.submit(history_A_stock_ktcap_chicang, tradedate_list)
            future15 = pool.submit(history_A_stock_ktcap_bankuai, tradedate_list)
            future16 = pool.submit(history_A_stock_ktcap_xiaopiao, tradedate_list)
            future17 = pool.submit(history_stock_MARKETVALUE, tradedate_list)

            def get_result(des, future):
                global_data_res[des] = future.result()

            future1.add_done_callback(functools.partial(get_result, "history_A_stock_top"))
            future2.add_done_callback(functools.partial(get_result, "history_A_stock_changenum"))
            # future3.add_done_callback(functools.partial(get_result, "history_A_stock_changenum_for3"))
            future4.add_done_callback(functools.partial(get_result, "history_A_stock_ktcap"))
            future5.add_done_callback(functools.partial(get_result, "history_A_stock_industry"))
            future6.add_done_callback(functools.partial(get_result, "history_stock_broker"))
            future7.add_done_callback(functools.partial(get_result, "history_stock_ktcap"))
            future8.add_done_callback(functools.partial(get_result, "history_stock_industry"))
            future9.add_done_callback(functools.partial(get_result, "history_A_stock_lv"))
            future10.add_done_callback(functools.partial(get_result, "banKuailist"))
            future11.add_done_callback(functools.partial(get_result, "aShardslist"))
            future12.add_done_callback(functools.partial(get_result, "stock_header_description"))
            future13.add_done_callback(functools.partial(get_result, "history_stock_holdnum_ktcap"))
            future14.add_done_callback(functools.partial(get_result, "history_A_stock_ktcap_chicang"))
            future15.add_done_callback(functools.partial(get_result, "history_A_stock_ktcap_bankuai"))
            future16.add_done_callback(functools.partial(get_result, "history_A_stock_ktcap_xiaopiao"))
            future17.add_done_callback(functools.partial(get_result, "history_stock_MARKETVALUE"))

        with pandas.ExcelWriter(bio, engine='xlsxwriter') as writer:
            des_df, stream_dict = global_data_res.get("stock_header_description", (pd.DataFrame(), None))
            if not des_df.empty:
                des_df.style.set_properties(
                    **{"align": "mid", "text-align": "center", "vertical-align": "middle"}).to_excel(
                    writer, sheet_name='北上资金说明(亿)', encoding="utf-8")
                workbook = writer.book
                completed_fmt = workbook.add_format({
                    'align': 'center',
                    'valign': 'vcenter',

                })
                format_color = workbook.add_format({'bg_color': 'gray'})

                worksheet = writer.sheets['北上资金说明(亿)']
                x, y = des_df.shape
                first_row = 2
                first_col = 2
                last_row = x
                last_col = 1 + y
                worksheet.conditional_format(0, 0, last_row, 1, {'type': 'no_blanks',
                                                                 'format': completed_fmt})
                worksheet.conditional_format(first_row, first_col, last_row, last_col, {'type': 'formula',
                                                                                        'criteria': '=mod(row(),2)=1',
                                                                                        'format': format_color})
                for wk, buffer in stream_dict.items():
                    worksheet.insert_image(wk, "filename", options={"image_data": buffer})
                workbook.add_vba_project('./vba/vbaProject.bin')
            history_A_stock_top_df = global_data_res.get("history_A_stock_top", pd.DataFrame())
            if not history_A_stock_top_df.empty:
                history_A_stock_top_df.style.applymap(highlight_background,
                                                      subset=history_A_stock_top_df.columns[:]).to_excel(writer,
                                                                                                         sheet_name='股票至今十大买入卖出(万)',
                                                                                                         encoding="utf-8",
                                                                                                         merge_cells=False)
            history_A_stock_changenum_df = global_data_res.get("history_A_stock_changenum", pd.DataFrame())
            if not history_A_stock_changenum_df.empty:
                history_A_stock_changenum_df = drop_duplicates_stockcode(history_A_stock_changenum_df)
                history_A_stock_changenum_df.style.applymap(highlight_background,
                                                            subset=history_A_stock_changenum_df.columns[:]).to_excel(
                    writer,
                    sheet_name='股票近10日买入量(万股)',
                    encoding="utf-8",
                    merge_cells=False)
            history_A_stock_changenum_df_for3 = history_A_stock_changenum_for3(history_A_stock_changenum_df,
                                                                               tradedate_list)
            if not history_A_stock_changenum_df_for3.empty:
                history_A_stock_changenum_df_for3 = drop_duplicates_stockcode(history_A_stock_changenum_df_for3)
                history_A_stock_changenum_df_for3.style.applymap(highlight_background,
                                                                 subset=history_A_stock_changenum_df_for3.columns[
                                                                        :]).to_excel(
                    writer,
                    sheet_name='当日买入为前三日两倍以上(万股)',
                    encoding="utf-8",
                    merge_cells=False)
            history_A_stock_ktcap_chicang_df = global_data_res.get("history_A_stock_ktcap_chicang", pd.DataFrame())
            if not history_A_stock_ktcap_chicang_df.empty:
                history_A_stock_ktcap_chicang_df = drop_duplicates_stockcode(history_A_stock_ktcap_chicang_df)
                history_A_stock_ktcap_chicang_df.style.applymap(highlight_background,
                                                                subset=history_A_stock_ktcap_chicang_df.columns[
                                                                       :]).to_excel(writer,
                                                                                    sheet_name='持仓(万)',
                                                                                    encoding="utf-8",
                                                                                    merge_cells=False,
                                                                                    engine='xlsxwriter')
            history_A_stock_ktcap_bankuai_df = global_data_res.get("history_A_stock_ktcap_bankuai", pd.DataFrame())
            if not history_A_stock_ktcap_bankuai_df.empty:
                history_A_stock_ktcap_bankuai_df = drop_duplicates_stockcode(history_A_stock_ktcap_bankuai_df)
                history_A_stock_ktcap_bankuai_df.style.applymap(highlight_background,
                                                                subset=history_A_stock_ktcap_bankuai_df.columns[
                                                                       :]).to_excel(writer,
                                                                                    sheet_name='预测(万)',
                                                                                    encoding="utf-8",
                                                                                    merge_cells=False,
                                                                                    engine='xlsxwriter')
            history_A_stock_ktcap_xiaopiao_df = global_data_res.get("history_A_stock_ktcap_xiaopiao", pd.DataFrame())
            if not history_A_stock_ktcap_xiaopiao_df.empty:
                history_A_stock_ktcap_xiaopiao_df = drop_duplicates_stockcode(history_A_stock_ktcap_xiaopiao_df)
                history_A_stock_ktcap_xiaopiao_df.style.applymap(highlight_background,
                                                                 subset=history_A_stock_ktcap_xiaopiao_df.columns[
                                                                        :]).to_excel(writer,
                                                                                     sheet_name='观察(万)',
                                                                                     encoding="utf-8",
                                                                                     merge_cells=False,
                                                                                     engine='xlsxwriter')

            history_A_stock_ktcap_df, trade_df_for5, trade_df_for5buy = global_data_res.get("history_A_stock_ktcap", (
                pd.DataFrame(), pd.DataFrame(), pd.DataFrame()))
            # history_A_stock_ktcap_df.style.background_gradient(subset=history_A_stock_ktcap_df.columns[:-3],
            # cmap="RdYlGn_r", high=0.2, low=0.1).applymap(highlight_background,
            # subset=history_A_stock_ktcap_df.columns[ -3:]).to_excel(writer, sheet_name='股票近10日买入额(万)',
            # encoding="utf-8", merge_cells=False, engine='xlsxwriter')
            if not history_A_stock_ktcap_df.empty:
                history_A_stock_ktcap_df = drop_duplicates_stockcode(history_A_stock_ktcap_df)
                history_A_stock_ktcap_df.style.applymap(highlight_background,
                                                        subset=history_A_stock_ktcap_df.columns[:]).to_excel(writer,
                                                                                                             sheet_name='股票近10日买入额(万)',
                                                                                                             encoding="utf-8",
                                                                                                             merge_cells=False,
                                                                                                             engine='xlsxwriter')
            if not trade_df_for5.empty:
                trade_df_for5 = drop_duplicates_stockcode(trade_df_for5)
                trade_df_for5.style.applymap(highlight_background,
                                             subset=trade_df_for5.columns[:]).to_excel(writer,
                                                                                       sheet_name='股票近10日买入额前五买入(万)',
                                                                                       encoding="utf-8",
                                                                                       merge_cells=False,
                                                                                       engine='xlsxwriter')
            if not trade_df_for5buy.empty:
                trade_df_for5buy = drop_duplicates_stockcode(trade_df_for5buy)
                trade_df_for5buy.style.applymap(highlight_background,
                                                subset=trade_df_for5buy.columns[:]).to_excel(writer,
                                                                                             sheet_name='股票近10日买入额连续五日大于0(万)',
                                                                                             encoding="utf-8",
                                                                                             merge_cells=False,
                                                                                             engine='xlsxwriter')

            history_A_stock_industry_df = global_data_res.get("history_A_stock_industry", pd.DataFrame())
            # history_A_stock_industry_df.style.background_gradient(subset=history_A_stock_industry_df.columns[:-3],
            # cmap="RdYlGn_r", high=0.2, low=0.1).applymap(highlight_background,
            # subset=history_A_stock_industry_df.columns[ -3:]).to_excel(writer, sheet_name='板块近10日买入额(万)',
            # encoding="utf-8", merge_cells=False, engine='xlsxwriter')
            if not history_A_stock_industry_df.empty:
                history_A_stock_industry_df.style.applymap(highlight_background,
                                                           subset=history_A_stock_industry_df.columns[:]).to_excel(
                    writer,
                    sheet_name='板块近10日买入额(万)',
                    encoding="utf-8",
                    merge_cells=False,
                    engine='xlsxwriter')
            history_stock_broker_df = global_data_res.get("history_stock_broker", pd.DataFrame())
            if not history_stock_broker_df.empty:
                history_stock_broker_df.style.applymap(highlight_background,
                                                       subset=history_stock_broker_df.columns[:]).to_excel(writer,
                                                                                                           sheet_name='经纪商近10日买入额(万)',
                                                                                                           encoding="utf-8",
                                                                                                           merge_cells=False,
                                                                                                           engine='xlsxwriter')
            history_stock_MARKETVALUE_df = global_data_res.get("history_stock_MARKETVALUE", pd.DataFrame())
            if not history_stock_MARKETVALUE_df.empty:
                history_stock_MARKETVALUE_df.style.applymap(highlight_background,
                                                            subset=history_stock_MARKETVALUE_df.columns[:]).to_excel(
                    writer,
                    sheet_name='经纪商近10日持仓持仓市值(万)',
                    encoding="utf-8",
                    merge_cells=False,
                    engine='xlsxwriter')
            history_stock_ktcap_df = global_data_res.get("history_stock_ktcap", pd.DataFrame())
            if not history_stock_ktcap_df.empty:
                history_stock_ktcap_df = drop_duplicates_stockcode(history_stock_ktcap_df)
                history_stock_ktcap_df.style.applymap(highlight_background,
                                                      subset=history_stock_ktcap_df.columns[:]).to_excel(writer,
                                                                                                         sheet_name='股票买卖相对自由流通股近10日变动率(%)',
                                                                                                         encoding="utf-8",
                                                                                                         merge_cells=False,
                                                                                                         engine='xlsxwriter')
            history_stock_industry_df = global_data_res.get("history_stock_industry", pd.DataFrame())
            if not history_stock_industry_df.empty:
                history_stock_industry_df.style.applymap(highlight_background,
                                                         subset=history_stock_industry_df.columns[:]).to_excel(writer,
                                                                                                               sheet_name='板块买卖相较于北上资金持有板块票总和近10日变动率(%)',
                                                                                                               encoding="utf-8",
                                                                                                               merge_cells=False,
                                                                                                               engine='xlsxwriter')
            history_A_stock_lv_df = global_data_res.get("history_A_stock_lv", pd.DataFrame())
            if not history_A_stock_lv_df.empty:
                history_A_stock_lv_df = drop_duplicates_stockcode(history_A_stock_lv_df)
                history_A_stock_lv_df.style.applymap(highlight_background,
                                                     subset=history_A_stock_lv_df.columns[:]).to_excel(writer,
                                                                                                       sheet_name='股票买卖相对昨日持仓变化率(%)',
                                                                                                       encoding="utf-8",
                                                                                                       merge_cells=False,
                                                                                                       engine='xlsxwriter')
            history_stock_holdnum_ktcap_df = global_data_res.get("history_stock_holdnum_ktcap", pd.DataFrame())
            if not history_stock_holdnum_ktcap_df.empty:
                history_stock_holdnum_ktcap_df = drop_duplicates_stockcode(history_stock_holdnum_ktcap_df)
                history_stock_holdnum_ktcap_df.style.applymap(highlight_background,
                                                              subset=history_stock_holdnum_ktcap_df.columns[
                                                                     :]).to_excel(writer,
                                                                                  sheet_name='股票近十日股票持仓相对流通股比例(%)',
                                                                                  encoding="utf-8", merge_cells=False)

        remote_path = "/home/guest/003-数据/001-经纪商数据"
        # remote_path = "/home/guest/tmp/excel"
        # remote_path = "/home/toptrade/file"
        vba_sftp_upload_xlsm(bio, remote_path, fordate)
        s2 = global_data_res.get("banKuailist")
        s3 = global_data_res.get("aShardslist")
        with ThreadPoolExecutor(max_workers=3) as pool:
            future21 = pool.submit(forpicture3, s3, fordate, f"./img/{fordate}-img")

            future22 = pool.submit(forpicture2, s2, fordate, f"./img/{fordate}-img-板块")

            def completed(local_path, remote_path, fordate, despath, future):
                sftp_upload_img(local_path, remote_path, fordate, despath)

            future21.add_done_callback(
                functools.partial(completed, f"./img/{fordate}-img", remote_path, fordate, f"{fordate}-img"))
            future22.add_done_callback(
                functools.partial(completed, f"./img/{fordate}-img-板块", remote_path, fordate, f"{fordate}-img-板块"))



    except Exception as e:
        logger.error(e)
        traceback.print_exc()


if __name__ == "__main__":
    tradedate = None
    run_img_excel_sftp(tradedate)
