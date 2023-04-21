# -*- coding: utf-8 -*-
"""
Created on

@author:
"""
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import List

import cx_Oracle as oracle
import os

import functools
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import paramiko
from io import BytesIO
import stat
import yaml
from matplotlib import font_manager
from matplotlib.font_manager import FontProperties
from sqlalchemy import create_engine, text
import traceback

from loguru import logger

logger.add("./log/img_excel_sftp_hk_data_pro-0414.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")
warnings.filterwarnings("ignore")
os.environ["NLS_LANG"] = "SIMPLIFIED CHINESE_CHINA.UTF8"
dns = oracle.makedsn("192.168.101.215", 1521, service_name="dazh")
engine = create_engine("oracle://fcdb:fcdb@" + dns, echo=True, pool_size=0,
                       max_overflow=-1)
logger.info("engine...")


class Load_configuration_info:

    def __init__(self, config_path):
        self.current_path = os.path.dirname(os.path.abspath(__file__))
        self.yaml_path = os.path.join(self.current_path, config_path)
        self.yaml_data = self.get_yaml_data()
        self.ssh_config = {
            'hostname': self.yaml_data.get("ssh", {}).get("hostname"),
            'port': self.yaml_data.get("ssh", {}).get("port"),
            'username': self.yaml_data.get("ssh", {}).get("username"),
            'password': self.yaml_data.get("ssh", {}).get("password"),
            'remote_path': self.yaml_data.get("ssh", {}).get("remote_path")
        }
        self.oracle_config = {
            'user': self.yaml_data.get("oracle", {}).get("user"),
            'password': self.yaml_data.get("oracle", {}).get("password"),
            'host': self.yaml_data.get("oracle", {}).get("host"),
            'port': self.yaml_data.get("oracle", {}).get("port"),
            'service_name': self.yaml_data.get("oracle", {}).get("service_name")
        }
        self.log_config = {
            'logpath': self.yaml_data.get("loginfo", {}).get("logpath"),
            'rotation': self.yaml_data.get("loginfo", {}).get("rotation"),
            'encoding': self.yaml_data.get("loginfo", {}).get("encoding"),
            'enqueue': self.yaml_data.get("loginfo", {}).get("enqueue"),
            'format': self.yaml_data.get("loginfo", {}).get("format"),
            'retention': self.yaml_data.get("loginfo", {}).get("retention")
        }

    def get_yaml_data(self):
        file = open(self.yaml_path, 'r', encoding="utf-8")
        file_data = file.read()
        file.close()
        data = yaml.load(file_data, Loader=yaml.Loader)
        logger.info("yaml_path...")
        return data


class Header(object):
    def __init__(self, tradedate_list):
        self.tradedate_list = tradedate_list
        self.buffer = BytesIO()

    def img_stream(self, df_dict):
        try:
            font = FontProperties(
                fname=r"/home/toptrade/project/ak_project/HK_data/hk_excel/SimHei.ttf",
                size=14)

            my_font = font_manager.FontProperties(
                fname=r"/home/toptrade/project/ak_project/HK_data/hk_excel/SimHei.ttf")

            stream_dict = {}
            for k, df in df_dict.items():
                if k == "record":
                    ax = df.T.plot(kind="bar", title=k, xlabel='日期', ylabel='数量', width=0.6)
                    ax.set_title(k, fontproperties=font)
                    ax.set_xlabel('日期', fontproperties=font)
                    ax.set_ylabel('数量', fontproperties=font)
                else:
                    ax = df.T.plot(kind="bar", title=k, xlabel='日期', ylabel='总额度(亿元)', width=0.6)
                    ax.set_title(k, fontproperties=font)
                    ax.set_xlabel('日期', fontproperties=font)
                    ax.set_ylabel('总额度(亿元)', fontproperties=font)
                ax.legend(prop=my_font, loc=2, bbox_to_anchor=(1.05, 1.0), borderaxespad=0., numpoints=1, fontsize=10)
                fig = ax.get_figure()
                fig.savefig(self.buffer, dpi=600, bbox_inches='tight')

                key = ""
                if k == "record":
                    key = "T2"
                elif k == "buy":
                    key = "T26"
                elif k == "sell":
                    key = "T49"
                elif k == "net_worth":
                    key = "T74"
                    key = "N2"
                stream_dict[key] = self.buffer
            return stream_dict
        except:
            return None

    def stock_header_description(self):
        """
        北上资金说明(亿)
        """
        try:
            record = pd.DataFrame(columns=self.tradedate_list)
            buy = pd.DataFrame(columns=self.tradedate_list)
            sell = pd.DataFrame(columns=self.tradedate_list)
            net_worth = pd.DataFrame(columns=self.tradedate_list)
            marketvalue = pd.DataFrame(columns=self.tradedate_list)
            for date in self.tradedate_list:
                header_sum__sql = text(f"""
        SELECT *
        FROM (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_60
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND SUBSTR(stockcode, 1, 2) = '60'
                            AND changenum > 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}'
             ),
             (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_00
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND SUBSTR(stockcode, 1, 2) = '00'
                            AND changenum > 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}'
             ),
             (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_30
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND SUBSTR(stockcode, 1, 2) = '30'
                            AND changenum > 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}'
             ),
             (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_688
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND SUBSTR(stockcode, 1, 3) = '688'
                            AND changenum > 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}'
             ),
             (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND changenum > 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}')
        UNION All
        SELECT *
        FROM (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_60
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND SUBSTR(stockcode, 1, 2) = '60'
                            AND changenum < 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}'
             ),
             (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_00
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND SUBSTR(stockcode, 1, 2) = '00'
                            AND changenum < 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}'
             ),
             (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_30
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND SUBSTR(stockcode, 1, 2) = '30'
                            AND changenum < 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}'
             ),
             (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_688
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND SUBSTR(stockcode, 1, 3) = '688'
                            AND changenum < 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}'
             ),
             (
                 SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose
                 FROM (
                          SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                          FROM T_O_HK_BROKERHOLDNUM_NEW
                          WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                            AND changenum < 0
                          GROUP BY stockcode, stockname
                      ) t
                          JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
                 WHERE q.tradedate = '{date}')
        UNION ALL
        SELECT *
        FROM (select count(1) AS hzb
              FROM T_O_HK_BROKERHOLDNUM_NEW
              WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                AND SUBSTR(stockcode, 1, 2) = '60') c1,
             (select count(1) AS szb
              FROM T_O_HK_BROKERHOLDNUM_NEW
              WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                AND SUBSTR(stockcode, 1, 2) = '00') s2,
             (select count(1) AS scy
              FROM T_O_HK_BROKERHOLDNUM_NEW
              WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                AND SUBSTR(stockcode, 1, 2) = '30') c3,
             (select count(1) AS hkc
              FROM T_O_HK_BROKERHOLDNUM_NEW
              WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                AND SUBSTR(stockcode, 1, 3) = '688') c4,
             (select count(1) AS alsh FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) c5
        UNION ALL
        SELECT *
        FROM (select ROUND(sum(marketvalue) / 100000000, 0) AS hzb
              FROM T_O_HK_BROKERHOLDNUM_NEW
              WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                AND SUBSTR(stockcode, 1, 2) = '60') c1,
             (select ROUND(sum(marketvalue) / 100000000, 0) AS szb
              FROM T_O_HK_BROKERHOLDNUM_NEW
              WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                AND SUBSTR(stockcode, 1, 2) = '00') s2,
             (select ROUND(sum(marketvalue) / 100000000, 0) AS scy
              FROM T_O_HK_BROKERHOLDNUM_NEW
              WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                AND SUBSTR(stockcode, 1, 2) = '30') c3,
             (select ROUND(sum(marketvalue) / 100000000, 0) AS hkc
              FROM T_O_HK_BROKERHOLDNUM_NEW
              WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                AND SUBSTR(stockcode, 1, 3) = '688') c4,
             (select ROUND(sum(marketvalue) / 100000000, 0) AS alsh
              FROM T_O_HK_BROKERHOLDNUM_NEW
              WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) c5
                """)
                df_header = pd.read_sql(header_sum__sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_header:
                    dflist.append(chunk)
                df_header = pd.concat(dflist)
                df_header.columns = ["沪主板", "深主板", "深创业", "沪科创", "汇总"]
                s = df_header.loc[0:1, :].sum()
                s = pd.DataFrame([s.to_dict()])
                df_header = pd.concat([df_header, s], ignore_index=True)
                record[date] = pd.DataFrame(df_header.iloc[2])
                buy[date] = pd.DataFrame(df_header.iloc[0])
                sell[date] = pd.DataFrame(df_header.iloc[1])
                net_worth[date] = pd.DataFrame(df_header.iloc[4])
                marketvalue[date] = pd.DataFrame(df_header.iloc[3])

            index_names = ['统计项', '统计汇总']
            record_fir_index = ["记录"]
            record_sec_index = ["沪主板", "深主板", "深创业", "沪科创", "汇总"]

            record_index = pd.MultiIndex.from_product([record_fir_index, record_sec_index], names=index_names)
            record.index = record_index

            buy_fir_index = ["买入"]
            buy_index = pd.MultiIndex.from_product([buy_fir_index, record_sec_index], names=index_names)
            buy.index = buy_index

            sell_fir_index = ["卖出"]
            sell_index = pd.MultiIndex.from_product([sell_fir_index, record_sec_index], names=index_names)
            sell.index = sell_index
            net_worth_fir_index = ["净值"]
            net_worth_index = pd.MultiIndex.from_product([net_worth_fir_index, record_sec_index], names=index_names)
            net_worth.index = net_worth_index
            marketvalue_fir_index = ["持仓市值"]
            marketvalue_index = pd.MultiIndex.from_product([marketvalue_fir_index, record_sec_index], names=index_names)
            marketvalue.index = marketvalue_index
            net_worth_plt = net_worth.copy()
            net_worth_plt.index = ["沪主板", "深主板", "深创业", "沪科创", "汇总"]
            df_dict = {"net_worth": net_worth_plt}
            stream_dict = self.img_stream(df_dict)
            trade_df = pd.concat([record, buy, sell, net_worth, marketvalue])

            return trade_df, stream_dict
        except:
            return pd.DataFrame(), None


class Hksftpdata(object):

    def __init__(self, tradedate, auto=True):
        logger.info("init...")
        self.start_time = time.time()
        self.load_config()
        logger.info("init completed...")
        self.tradedate = tradedate
        self.global_data_res = {}
        self.bio = BytesIO()
        if auto:
            self.func_tradeday_changenum()
        self.tradedate_list = self.get_tradedate_list_rownum(tradedate)
        self.header = Header(self.tradedate_list)
        self.run()

    def func_tradeday_changenum(self):
        tradedate_list = self.get_tradedate_add_table()
        if len(tradedate_list) > 0:
            if self.tradedate is not None:
                tradedate = self.tradedate
            else:
                tradedate = tradedate_list[-1]

            logger.info(tradedate)
            conn = engine.raw_connection()
            cursor = conn.cursor()
            # completed = cursor.callfunc('FUNC_T_O_HK_BRO_INSERTDATA', oracle.STRING, [tradedate])
            # logger.info(f"tradedate:{tradedate},FUNC_T_O_HK_BRO_INSERTDATA:{completed}")
            logger.info(
                f"SELECT  COUNT(*)  FROM  T_O_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd')")
            cursor.execute(
                f"SELECT  COUNT(*)  FROM  T_O_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd')")
            result = cursor.fetchone()
            logger.info(f"tradedate:{tradedate},T_O_HK_BROKERHOLDNUM_NEW_COUNT:{result}")
            if result:
                if result[0] == 0:
                    completed = cursor.callfunc('FUNC_T_O_HK_BRO_INSERTDATA', oracle.STRING, [tradedate])
                    logger.info(f"tradedate:{tradedate},FUNC_T_O_HK_BRO_INSERTDATA:{completed}")
                else:
                    pass
                    # cursor.execute(f"DELETE FROM  T_O_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd')")
                    # logger.info(f"DELETE FROM  T_O_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd')")
                    # logger.info(f"tradedate:{tradedate},DELETE:T_O_HK_BROKERHOLDNUM_NEW")
                    # cursor.callproc("T_O_HK_lugutongstock")
                    # logger.info(f"tradedate:{tradedate},T_O_HK_lugutongstock:completed")
            completed = cursor.callfunc('FUNC_T_O_HK_BRO_DEL_REP', oracle.STRING, [tradedate])
            logger.info(f"tradedate:{tradedate},FUNC_T_O_HK_BRO_DEL_REP:{completed}")
            cursor.close()
            logger.info(f"cursor.close()")
            conn.close()
            logger.info(f"conn.close()")
        else:
            logger.info(f"tradedate:{self.tradedate_list[-1]},FUNC_TRADEDAY_CHANGENUM:无更新条目")

    def load_config(self):
        # logger.remove(handler_id=None)
        conf_fields = Load_configuration_info("bjhy_config.yaml")
        self.ssh_config = conf_fields.ssh_config

    def run(self):
        self.run_img_excel_sftp()

    def get_benrate(self, series):
        changenum = series['changenum']
        totmktcap = series['totmktcap']
        if totmktcap == 0:
            return 0
        else:
            return changenum / totmktcap

    def get_marketvalue_rate(self, series):
        changenum = series['changenum']
        totmktcap = series['totmktcap']
        AVGPRICE = series['AVGPRICE']
        if totmktcap == 0:
            return 0
        else:
            return (changenum * AVGPRICE) / (totmktcap * 10000)

    def get_industry_rate(self, series):
        changenum_AVGPRICE = series['changenum_AVGPRICE']
        totmktcap = series['totmktcap']
        if totmktcap == 0:
            return 0
        else:
            return changenum_AVGPRICE / totmktcap

    def stock_ktcap_rate(self, series):
        df = series.str.split(",", expand=True)
        df.columns = ["sum_changenum", "AVGPRICE", "totmktcap"]
        df['sum_changenum'] = df['sum_changenum'].astype(np.float64)
        df['AVGPRICE'] = df['AVGPRICE'].astype(np.float64)
        df['totmktcap'] = df['totmktcap'].astype(np.float64)
        return round(df["sum_changenum"] / df["totmktcap"] * 100 if df['totmktcap'].sum() != 0 else 0.0, 2)

    def stock_ktcap_day_rate(self, x):
        df = x.str.split(",", expand=True)
        if df.shape[1] == 3:
            df.columns = ["sum_changenum", "AVGPRICE", "totmktcap"]
            df['sum_changenum'] = df['sum_changenum'].astype(np.float64)
            df['AVGPRICE'] = df['AVGPRICE'].astype(np.float64)
            df['totmktcap'] = df['totmktcap'].astype(np.float64)
            cap = df['sum_changenum'].sum() / df['totmktcap'].iloc[0,] * 100 if df['totmktcap'].sum() != 0 else 0.0
            return round(cap, 2)

    def history_A_stock_changenum(self):
        """
        股票近10日买入量(万股)
        """
        try:
            trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
            trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            for date in self.tradedate_list:
                sql = text(f'''select t.stockcode, case substr(stockname, 1, 2) when 'DR' then (select distinct (stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) 
                when 'XD' then (select distinct (stockname) from T_O_HK_BROKERHOLDNUM_NEW f where f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) 
                else stockname end stockname, industryname, ROUND(g.TOTALSHARE / 10000, 2) TOTALSHARE, ROUND(g.CIRCSKAMT / 10000, 2) CIRCSKAMT, ROUND(ft.holdnum/100 / g.CIRCSKAMT , 2) holdnum, sum(changenum) totalnu 
                from T_O_HK_BROKERHOLDNUM_NEW t, TQ_SK_SHARESTRUCHG g, TQ_SK_BASICINFO s, 
                (select a.stockcode, sum(a.holdnum) holdnum from T_O_HK_BROKERHOLDNUM_NEW a where a.tradeday = to_date({self.tradedate_list[-1]}, 'yyyy/mm/dd') GROUP BY a.stockcode) ft  
                where t.tradeday = to_date({date}, 'yyyy/mm/dd') and t.stockcode = s.symbol and g.compcode = s.compcode and t.stockcode = ft.stockcode and g.ENTRYDATE = (select max(g.ENTRYDATE) 
                from TQ_SK_SHARESTRUCHG g, TQ_SK_BASICINFO f where g.compcode = f.compcode and f.symbol = s.symbol) and g.begindate = (select max(g.begindate) from TQ_SK_SHARESTRUCHG g, TQ_SK_BASICINFO f 
                where g.compcode = f.compcode and f.symbol = s.symbol) GROUP BY t.stockcode, stockname, industryname, ROUND(ft.holdnum/100 / g.CIRCSKAMT , 2), ROUND(g.TOTALSHARE / 10000, 2), ROUND(g.CIRCSKAMT / 10000, 2)''')
                df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_changenum_list:
                    dflist.append(chunk)
                df_changenum = pd.concat(dflist)
                df_changenum.columns = ["stockcode", "stockname", "industryname", "TOTALSHARE", "CIRCSKAMT", "持仓流通占比",
                                        date]
                df_changenum.set_index(["stockcode", 'stockname', "industryname", "TOTALSHARE", "CIRCSKAMT", "持仓流通占比"],
                                       drop=True,
                                       append=False, inplace=True)
                trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            trade_df.sort_values(by=self.tradedate_list, ascending=False, inplace=True)
            trade_df['近三天'] = trade_df[self.tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近五天'] = trade_df[self.tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近十天'] = trade_df[self.tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
            # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
            trade_df = trade_df.apply(lambda x: (x / 10000).round())
            trade_df.fillna(0, inplace=True)
            return trade_df
        except:
            return pd.DataFrame()

    def history_stock_ktcap(self):
        """
        股票相对流通股近10日变动率(%)
        """
        try:
            trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
            trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            for date in self.tradedate_list:
                sql = text(f"""SELECT t.stockcode,t.stockname,t.industryname,t.sum_changenum ||','|| q.CIRCSKAMT ||',' || q.CIRCSKAMT * 10000 AS text 
                FROM (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' 
                then (select distinct(stockname) from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) 
                else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  FROM T_O_HK_BROKERHOLDNUM_NEW t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') 
                GROUP BY stockcode,stockname,industryname) t, TQ_SK_SHARESTRUCHG q, TQ_SK_BASICINFO s WHERE q.compcode =  s.compcode  AND t.stockcode=s.SYMBOL and  q.ENTRYDATE = ( select max(g.ENTRYDATE) 
                from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO l where g.compcode = l.compcode and l.symbol = s.symbol)  and  q.begindate = ( select max(g.begindate) 
                from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO l where g.compcode = l.compcode and l.symbol = s.symbol) and t.sum_changenum != 0""")
                df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_changenum_list:
                    dflist.append(chunk)
                df_changenum = pd.concat(dflist)
                df_changenum.columns = ["stockcode", "stockname", "industryname", date]
                df_changenum.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False,
                                       inplace=True)
                trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
            trade_df.fillna("0,1,1", inplace=True)

            df = trade_df.apply(self.stock_ktcap_rate)
            trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            df['近三天'] = trade_df.loc[:, self.tradedate_list[-3:]].apply(self.stock_ktcap_day_rate, axis=1)
            df['近五天'] = trade_df.loc[:, self.tradedate_list[-5:]].apply(self.stock_ktcap_day_rate, axis=1)
            df['近十天'] = trade_df.loc[:, self.tradedate_list[0:10]].apply(self.stock_ktcap_day_rate, axis=1)
            df.fillna(0, inplace=True)
            df = df.loc[(df.loc[:, self.tradedate_list[0:10]].sum(axis=1) != 0)]
            return df
        except:
            return pd.DataFrame()

    def history_A_stock_industry(self):
        """
        板块近10日买入额(万)
        """
        try:
            trade_df = pd.DataFrame(columns=["industrycode", "industryname"])
            trade_df.set_index(["industrycode", 'industryname'], drop=True, append=False, inplace=True)
            for date in self.tradedate_list:
                industry_totmktcap_sql = text(f'''
                    SELECT t.industrycode,t.industryname,ROUND(SUM(t.changenum*q.AVGPRICE)) AS sum_mul_chclose FROM (
                    SELECT industrycode,industryname,stockcode,stockname,changenum FROM T_O_HK_BROKERHOLDNUM_NEW 
                    WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s 
                    WHERE q.secode =  s.secode  AND t.stockcode=s.SYMBOL  AND q.tradedate={date} GROUP BY t.industryname,t.industrycode''')
                df_industry_list = pd.read_sql(industry_totmktcap_sql, engine.connect(), chunksize=10000)
                dflist = []
                for chunk in df_industry_list:
                    dflist.append(chunk)
                df_industry = pd.concat(dflist)
                df_industry.columns = ["industrycode", "industryname", date]
                df_industry.set_index(["industrycode", 'industryname'], drop=True, append=False, inplace=True)
                trade_df = pd.merge(trade_df, df_industry, how='outer', left_index=True, right_index=True)
                if ('0', '0') in trade_df.index.tolist():
                    trade_df.drop(index=('0', '0'), inplace=True)
            trade_df['近三天'] = trade_df[self.tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近五天'] = trade_df[self.tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近十天'] = trade_df[self.tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
            trade_df = trade_df.apply(lambda x: (x / 10000).round())
            trade_df.fillna(0, inplace=True)
            return trade_df
        except:
            return pd.DataFrame()

    def history_stock_rate(self, s):
        df = s.str.split(",", expand=True)
        df.columns = ["sum_mul_chclose", "sum_totmktcap"]
        df['sum_mul_chclose'] = df['sum_mul_chclose'].astype(np.float64)
        df['sum_totmktcap'] = df['sum_totmktcap'].astype(np.float64)
        return round(df['sum_mul_chclose'] / df['sum_totmktcap'] * 100, 2)

    def history_stock_day_rate(self, x):
        df = x.str.split(",", expand=True)
        df.columns = ["sum_mul_chclose", "sum_totmktcap"]
        df['sum_mul_chclose'] = df['sum_mul_chclose'].astype(np.float64)
        df['sum_totmktcap'] = df['sum_totmktcap'].astype(np.float64)
        ktcap = df['sum_mul_chclose'].sum() / df['sum_totmktcap'].sum() * 100 if df['sum_totmktcap'].sum() != 0 else 0
        return round(ktcap, 2)

    def history_stock_industry(self):
        """
        板块相较于申万板块近10日变动率(%)
        """
        try:
            trade_df = pd.DataFrame(columns=["industrycode", "industryname"])
            trade_df.set_index(["industrycode", 'industryname'], drop=True, append=False, inplace=True)
            for date in self.tradedate_list:
                industry_totmktcap_sql = text(f"""
                    SELECT tt.industrycode,tt.industryname,tt.sum_mul_chclose ||','|| qq.sum_totmktcap FROM (
                    SELECT t.industrycode,t.industryname,SUM(t.changenum*q.AVGPRICE) AS sum_mul_chclose FROM (
                    SELECT industrycode,industryname,stockcode,stockname,changenum FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) t, 
                    TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s WHERE q.secode =  s.secode  AND t.stockcode=s.SYMBOL AND q.tradedate={date} GROUP BY t.industryname,t.industrycode
                    ) tt,
                    (
                    SELECT t.industrycode,t.industryname,SUM(q.totmktcap) AS sum_totmktcap FROM TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s,T_O_HK_BROKERHOLDNUM_NEW t 
                    WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date} GROUP BY t.industryname,t.industrycode
                    ) qq 
                    WHERE tt.industrycode = qq.industrycode""")
                df_industry_list = pd.read_sql(industry_totmktcap_sql, engine.connect(), chunksize=10000)
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
            df = trade_df.apply(self.history_stock_rate)
            df['近三天'] = trade_df.loc[:, self.tradedate_list[-3:]].apply(self.history_stock_day_rate, axis=1)
            df['近五天'] = trade_df.loc[:, self.tradedate_list[-5:]].apply(self.history_stock_day_rate, axis=1)
            df['近十天'] = trade_df.loc[:, self.tradedate_list[0:10]].apply(self.history_stock_day_rate, axis=1)
            df.fillna(0, inplace=True)
            df = df.loc[(df.loc[:, self.tradedate_list[0:10]].sum(axis=1) != 0)]
            return df
        except:
            return pd.DataFrame()

    def history_stock_broker(self):
        """
        经纪商近10日买入额(万)
        """
        try:
            trade_df = pd.DataFrame(columns=["brokercode", "brokername"])
            trade_df.set_index(["brokercode", 'brokername'], drop=True, append=False, inplace=True)
            for date in self.tradedate_list:
                broker_sql = text(f'''
                    SELECT t.brokercode,t.brokername,ROUND(SUM(q.AVGPRICE*t.changenum)) AS sum_mul_chclose
                    FROM    T_O_HK_BROKERHOLDNUM_NEW t,TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s 
                    WHERE   q.secode =  s.secode AND t.stockcode=s.SYMBOL AND q.tradedate ={date} AND tradeday = to_date({date}, 'yyyy/mm/dd') GROUP BY t.brokercode,t.brokername
                    ''')
                df_broker_list = pd.read_sql(broker_sql, engine.connect(), chunksize=10000)
                dflist = []
                for chunk in df_broker_list:
                    dflist.append(chunk)
                df_broker = pd.concat(dflist)
                df_broker.columns = ["brokercode", "brokername", date]
                df_broker.set_index(["brokercode", 'brokername'], drop=True, append=False, inplace=True)
                trade_df = pd.merge(trade_df, df_broker, how='outer', left_index=True, right_index=True)
            trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            trade_df.dropna(how='all', inplace=True)
            trade_df.sort_values(by=self.tradedate_list, ascending=False, inplace=True)
            trade_df = trade_df.apply(lambda x: (x // 10000))
            trade_df['近三天'] = trade_df[self.tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近五天'] = trade_df[self.tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近十天'] = trade_df[self.tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
            return trade_df
        except:
            return pd.DataFrame()

    def create_dir(self, sftp, remoteDir):
        try:
            if stat.S_ISDIR(sftp.stat(remoteDir).st_mode):
                pass
        except Exception as e:
            sftp.mkdir(remoteDir)
            print("在远程sftp上创建目录：{}".format(remoteDir))

    def sftp_upload(self, sftp, localDir, remoteDir):
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
                self.sftp_upload(sftp, localDirTmp, remoteDirTmp)
        else:
            print("upload file:", localDir)
            logger.info("upload file:" + localDir)
            try:
                sftp.put(localDir, remoteDir)
            except Exception as e:
                print('upload error:', e)

    def vba_sftp_upload_xlsm(self, remote_path, fordate):
        """
        上传excel文件，流方式
        """
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.ssh_config.get("hostname"), username=self.ssh_config.get("username"),
                               password=self.ssh_config.get("password"))
            stdin, stdout, stderr = ssh_client.exec_command(
                f"echo {self.ssh_config.get('password')} | sudo -S chmod 777 {self.ssh_config.get('remote_path')} -R",
                timeout=300)
            out = stdout.readlines()
            ftp_client = ssh_client.open_sftp()
            xlsm = open('./xlsm' + '/A股额度' + fordate + '.xlsm', "wb")
            with ftp_client.open(remote_path + '/A股额度' + fordate + '.xlsm', "w") as f:
                self.bio.seek(0)
                xlsm_byes = self.bio.read()
                xlsm.write(xlsm_byes)
                f.write(xlsm_byes)
                logger.info('/A股额度' + fordate + '.xlsm')
        except Exception as e:
            logger.error(e)

    def sftp_upload_img(self, local_path, remote_path, fordate, despath):
        """
        上传图片
        """
        try:
            localDir = local_path  # 本地文件或目录
            remoteDir = f'{remote_path}/{despath}/'  #
            sf = paramiko.Transport((self.ssh_config.get("hostname"), self.ssh_config.get("port")))
            sf.connect(username=self.ssh_config.get("username"), password=self.ssh_config.get("password"))
            sftp = paramiko.SFTPClient.from_transport(sf)
            self.sftp_upload(sftp, localDir, remoteDir)
            sf.close()
        except Exception as e:
            logger.error(e)

    def get_tradedate_list_month(self):
        """
        获取本月交易日
        """
        tradedate_sql = "SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_HK_BROKERHOLDNUM_NEW WHERE to_char(tradeday,'mm')=to_char(sysdate,'mm') AND to_char(tradeday,'yyyy')=to_char(sysdate,'yyyy') ORDER BY TRADEDAY desc) tr"
        df_tradedate = pd.read_sql(tradedate_sql, engine.connect())
        tradedate_list = sorted(df_tradedate["tradeday"].tolist())
        if len(tradedate_list) == 0:
            tradedate_sql = "SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_HK_BROKERHOLDNUM_NEW ORDER BY TRADEDAY desc) tr WHERE rownum<=10"
            df_tradedate = pd.read_sql(tradedate_sql, engine.connect())
            tradedate_list = sorted(df_tradedate["tradeday"].tolist())
        return tradedate_list

    def get_tradedate_list_rownum(self, tradedate: str = None) -> List[str]:
        """
        获取10个交易日
        """
        try:
            tradedate_sql = text(
                "SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_HK_BROKERHOLDNUM_NEW ORDER BY TRADEDAY desc) tr WHERE rownum<=10")
            if tradedate:
                tradedate_sql = text(
                    f"SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_HK_BROKERHOLDNUM_NEW ORDER BY TRADEDAY desc) tr WHERE tradeday <= to_date({tradedate}, 'yyyy/mm/dd') AND rownum<=10")
            df_tradedate = pd.read_sql(tradedate_sql, engine.connect())
            tradedate_list = sorted(df_tradedate["tradeday"].tolist())
            return tradedate_list
        except Exception as e:
            logger.error(e)

    def get_tradedate_add_table(self, tradedate: str = None) -> List[str]:
        """
        获取10个交易日
        """
        try:
            tradedate_sql = text("SELECT to_char(MAX(TRADEDAY),'yyyymmdd') AS TRADEDAY FROM T_HK_BROKERHOLDNUM_NEW")
            if tradedate:
                tradedate_sql = text(
                    f"SELECT to_char(MAX(TRADEDAY),'yyyymmdd') AS TRADEDAY FROM T_HK_BROKERHOLDNUM_NEW WHERE TRADEDAY=to_date({tradedate}, 'yyyy/mm/dd')")
            df_tradedate = pd.read_sql(tradedate_sql, engine.connect())
            tradedate_list = sorted(df_tradedate["tradeday"].tolist())
            return tradedate_list
        except Exception as e:
            logger.error(e)

    def highlight_background(self, val):
        """
        设置背景颜色
        """
        color = "#F9FF51"
        if val > 0:
            color = "#FF350C"
        elif val < 0:
            color = "#24FF1C"

        return 'background-color: %s' % color

    def history_A_stock_ktcap(self):
        """
        股票近10日买入额(万)
        """
        try:
            trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
            trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            # and t.changenum !=0
            for date in self.tradedate_list:
                sql = text(f'''select a1.stockcode,a1.stockname,a1.industryname,ROUND(a2.TOTMKTCAP / 10000),ROUND(a2.NEGOTIABLEMV / 10000),a2.marketvalue,ROUND(a2.marketvalue / ROUND(a2.NEGOTIABLEMV / 10000) * 100,2),a1.mul_totmktcap  
                from (SELECT t.stockcode,t.stockname,industryname, ROUND(t.sum_changenum * q.AVGPRICE) AS mul_totmktcap FROM  
                (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_HK_BROKERHOLDNUM_NEW f 
                where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) when 'TC' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'TC%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  
                FROM T_O_HK_BROKERHOLDNUM_NEW t WHERE tradeday = to_date({date}, 'yyyy/mm/dd')  GROUP BY stockcode,stockname,industryname) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s 
                WHERE q.secode =  s.secode  AND  t.stockcode=s.SYMBOL AND q.tradedate={date} ) a1 , (SELECT t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV,sum(t.marketvalue) /100000000 marketvalue 
                from TQ_QT_SKDAILYPRICE l, TQ_SK_BASICINFO s, T_O_HK_BROKERHOLDNUM_NEW t  where t.tradeday = to_date({self.tradedate_list[-1]}, 'yyyy/mm/dd')  and l.secode =  s.secode AND t.stockcode=s.SYMBOL AND l.tradedate={self.tradedate_list[-1]} 
                GROUP BY t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV) a2 where a1.stockcode = a2.stockcode''')
                df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_changenum_list:
                    dflist.append(chunk)
                df_changenum = pd.concat(dflist)
                df_changenum.columns = ["stockcode", "stockname", "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)",
                                        "持仓占比流通(%)",
                                        date]
                df_changenum.set_index(
                    ["stockcode", 'stockname', "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)"], drop=True,
                    append=False, inplace=True)
                trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            trade_df.sort_values(by=self.tradedate_list, ascending=False, inplace=True)
            trade_df['近三天'] = trade_df[self.tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近五天'] = trade_df[self.tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近十天'] = trade_df[self.tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
            # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
            trade_df = trade_df.apply(lambda x: (x / 10000).round()).sort_index()
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
            # 获取前5
            trade_df_for5 = trade_df[trade_df['近五天'] > 0].copy()
            trade_df_for5.sort_values(by='industryname', ascending=False, inplace=True)

            trade_df_for5 = trade_df_for5.groupby(['industryname']).head(5)

            # 连续5天买入
            trade_df_for5buy = trade_df[
                (trade_df[self.tradedate_list[-1]] > 0) & (trade_df[self.tradedate_list[-2]] > 0) & (
                        trade_df[self.tradedate_list[-3]] > 0) & (trade_df[self.tradedate_list[-4]] > 0) & (
                        trade_df[self.tradedate_list[-5]] > 0)].copy()
            return trade_df, trade_df_for5, trade_df_for5buy
        except:
            return pd.DataFrame()

    def history_A_stock_ktcap_chicang(self):
        """
        持仓股票近10日买入额(万)
        """
        try:
            trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
            trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            # and t.changenum !=0
            for date in self.tradedate_list:
                sql = text(f'''select a1.stockcode,a1.stockname,a1.industryname,ROUND(a2.TOTMKTCAP / 10000),ROUND(a2.NEGOTIABLEMV / 10000),a2.marketvalue,ROUND(a2.marketvalue / ROUND(a2.NEGOTIABLEMV / 10000) * 100,2),a1.mul_totmktcap  
                from (SELECT t.stockcode,t.stockname,industryname, ROUND(t.sum_changenum * q.AVGPRICE) AS mul_totmktcap 
                FROM  (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) when 'TC' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'TC%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  
                FROM T_O_HK_BROKERHOLDNUM_NEW t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode in (select substr(stockcode, 3, 6) 
                from T_O_FORUSSTOCK t where t.forstat = '持仓' and t.insertday = to_date({self.tradedate_list[- 1]}, 'yyyy/mm/dd'))  GROUP BY stockcode,stockname,industryname) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s 
                WHERE q.secode =  s.secode  AND  t.stockcode=s.SYMBOL AND q.tradedate={date} ) a1 , (SELECT t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV,sum(t.marketvalue) /100000000 marketvalue 
                from TQ_QT_SKDAILYPRICE l, TQ_SK_BASICINFO s, T_O_HK_BROKERHOLDNUM_NEW t  where t.tradeday = to_date({self.tradedate_list[-1]}, 'yyyy/mm/dd')  and l.secode =  s.secode AND t.stockcode=s.SYMBOL AND l.tradedate={self.tradedate_list[-1]} 
                GROUP BY t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV) a2 where a1.stockcode = a2.stockcode''')
                df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_changenum_list:
                    dflist.append(chunk)
                df_changenum = pd.concat(dflist)
                df_changenum.columns = ["stockcode", "stockname", "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)",
                                        "持仓占比流通(%)",
                                        date]
                df_changenum.set_index(
                    ["stockcode", 'stockname', "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)"], drop=True,
                    append=False, inplace=True)
                trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            trade_df.sort_values(by=self.tradedate_list, ascending=False, inplace=True)
            trade_df['近三天'] = trade_df[self.tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近五天'] = trade_df[self.tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近十天'] = trade_df[self.tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
            # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
            trade_df = trade_df.apply(lambda x: (x / 10000).round()).sort_index()
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
            return trade_df
        except:
            return pd.DataFrame()

    def history_A_stock_ktcap_bankuai(self):
        """
        板块股票近10日买入额(万)
        """
        try:
            trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
            trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            # and t.changenum !=0
            for date in self.tradedate_list:
                sql = text(f'''select a1.stockcode,a1.stockname,a1.industryname,ROUND(a2.TOTMKTCAP / 10000),ROUND(a2.NEGOTIABLEMV / 10000),a2.marketvalue,ROUND(a2.marketvalue / ROUND(a2.NEGOTIABLEMV / 10000) * 100,2),a1.mul_totmktcap  
                from (SELECT t.stockcode,t.stockname,industryname, ROUND(t.sum_changenum * q.AVGPRICE) AS mul_totmktcap 
                FROM  (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) when 'TC' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'TC%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  
                FROM T_O_HK_BROKERHOLDNUM_NEW t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode in (select substr(stockcode, 3, 6) 
                from T_O_FORUSSTOCK t where t.forstat = '预测' and t.insertday = to_date({self.tradedate_list[- 1]}, 'yyyy/mm/dd'))  
                GROUP BY stockcode,stockname,industryname) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s WHERE q.secode =  s.secode  AND  t.stockcode=s.SYMBOL AND q.tradedate={date} ) a1 , 
                (SELECT t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV,sum(t.marketvalue) /100000000 marketvalue from TQ_QT_SKDAILYPRICE l, TQ_SK_BASICINFO s, T_O_HK_BROKERHOLDNUM_NEW t  
                where t.tradeday = to_date({self.tradedate_list[-1]}, 'yyyy/mm/dd')  and l.secode =  s.secode AND t.stockcode=s.SYMBOL AND l.tradedate={self.tradedate_list[-1]} 
                GROUP BY t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV) a2 where a1.stockcode = a2.stockcode''')
                df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_changenum_list:
                    dflist.append(chunk)
                df_changenum = pd.concat(dflist)
                df_changenum.columns = ["stockcode", "stockname", "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)",
                                        "持仓占比流通(%)",
                                        date]
                df_changenum.set_index(
                    ["stockcode", 'stockname', "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)"], drop=True,
                    append=False, inplace=True)
                trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            trade_df.sort_values(by=self.tradedate_list, ascending=False, inplace=True)
            trade_df['近三天'] = trade_df[self.tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近五天'] = trade_df[self.tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近十天'] = trade_df[self.tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
            # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
            trade_df = trade_df.apply(lambda x: (x / 10000).round()).sort_index()
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
            return trade_df
        except:
            return pd.DataFrame()

    def history_A_stock_ktcap_xiaopiao(self):
        """
        观察股票近10日买入额(万)
        """
        try:
            trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
            trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            # and t.changenum !=0
            for date in self.tradedate_list:
                sql = text(f'''select a1.stockcode,a1.stockname,a1.industryname,ROUND(a2.TOTMKTCAP / 10000),ROUND(a2.NEGOTIABLEMV / 10000),a2.marketvalue,ROUND(a2.marketvalue / ROUND(a2.NEGOTIABLEMV / 10000) * 100,2),a1.mul_totmktcap  
                from (SELECT t.stockcode,t.stockname,industryname, ROUND(t.sum_changenum * q.AVGPRICE) AS mul_totmktcap 
                FROM  (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_HK_BROKERHOLDNUM_NEW f 
                where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) when 'TC' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'TC%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(changenum) AS sum_changenum  
                FROM T_O_HK_BROKERHOLDNUM_NEW t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode in (select substr(stockcode, 3, 6) from T_O_FORUSSTOCK t where t.forstat = '观察' and t.insertday = to_date({self.tradedate_list[- 1]}, 'yyyy/mm/dd'))  
                GROUP BY stockcode,stockname,industryname) t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s WHERE q.secode =  s.secode  AND  t.stockcode=s.SYMBOL AND q.tradedate={date} ) a1 , 
                (SELECT t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV,sum(t.marketvalue) /100000000 marketvalue from TQ_QT_SKDAILYPRICE l, TQ_SK_BASICINFO s, T_O_HK_BROKERHOLDNUM_NEW t  
                where t.tradeday = to_date({self.tradedate_list[-1]}, 'yyyy/mm/dd')  and l.secode =  s.secode AND t.stockcode=s.SYMBOL AND l.tradedate={self.tradedate_list[-1]} 
                GROUP BY t.stockcode,t.stockname,l.TOTMKTCAP,l.NEGOTIABLEMV) a2 where a1.stockcode = a2.stockcode''')
                df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_changenum_list:
                    dflist.append(chunk)
                df_changenum = pd.concat(dflist)
                df_changenum.columns = ["stockcode", "stockname", "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)",
                                        "持仓占比流通(%)",
                                        date]
                df_changenum.set_index(
                    ["stockcode", 'stockname', "industryname", "总市值(亿)", "流通值(亿)", "持仓额(亿)", "持仓占比流通(%)"], drop=True,
                    append=False, inplace=True)
                trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            trade_df.sort_values(by=self.tradedate_list, ascending=False, inplace=True)
            trade_df['近三天'] = trade_df[self.tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近五天'] = trade_df[self.tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
            trade_df['近十天'] = trade_df[self.tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
            # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
            trade_df = trade_df.apply(lambda x: (x / 10000).round()).sort_index()
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.loc[:, tradedate_list[0:10]].sum(axis=1) != 0)]
            return trade_df
        except:
            return pd.DataFrame()

    # 当日买入为前3日两倍以上（万股）
    def history_A_stock_changenum_for3(self, trade_df):
        try:
            trade_df = trade_df.loc[
                (trade_df.loc[:, self.tradedate_list[-4:-1]].sum(axis=1) < trade_df.loc[:,
                                                                           self.tradedate_list[-1:]].sum(
                    axis=1) / 2)]
            trade_df = trade_df.loc[
                trade_df.loc[:, self.tradedate_list[-4:-1]].sum(axis=1) > 0]
            return trade_df
        except:
            return pd.DataFrame()

    # 买卖前10
    def history_A_stock_top(self):
        """
        股票至今十大买入卖出(万)
        """
        try:
            trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
            trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            for date in self.tradedate_list:
                sql = text(f'''select tag,stockcode,stockname,industryname,total from 
                (select 'buy' tag,to_char(t.tradeday , 'YYYY-mm-dd') dd,stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1)  else stockname end stockname,industryname, row_number() 
                OVER(PARTITION BY to_char(t.tradeday , 'YYYY-mm-dd') ORDER BY sum(changenum)*q.AVGPRICE desc) rn ,sum(changenum)*q.AVGPRICE total 
                from T_O_HK_BROKERHOLDNUM_NEW t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s where q.secode =  s.secode and s.symbol = t.stockcode 
                and q.tradedate = to_char(t.tradeday, 'YYYYmmdd') and  tradeday = to_date({date}, 'yyyy/mm/dd') and changenum > 0  group by stockcode,stockname,industryname,q.AVGPRICE,to_char(t.tradeday , 'YYYY-mm-dd')) where rn < 11
                    union
                    select tag,stockcode,stockname,industryname,total from (select 'sell' tag,to_char(t.tradeday , 'YYYY-mm-dd') dd,stockcode,case substr(stockname,1,2) 
                    when 'DR' then (select distinct(stockname) from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) 
                    when 'XD' then (select distinct(stockname) from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) 
                    else stockname end stockname,industryname, row_number() OVER(PARTITION BY to_char(t.tradeday , 'YYYY-mm-dd') ORDER BY sum(changenum)*q.AVGPRICE ) rn ,sum(changenum)*q.AVGPRICE total 
                    from T_O_HK_BROKERHOLDNUM_NEW t, TQ_QT_SKDAILYPRICE q, TQ_SK_BASICINFO s where q.secode =  s.secode and s.symbol = t.stockcode and q.tradedate = to_char(t.tradeday, 'YYYYmmdd') and  
                    tradeday = to_date({date}, 'yyyy/mm/dd') and changenum < 0  group by stockcode,stockname,industryname,q.AVGPRICE,to_char(t.tradeday , 'YYYY-mm-dd')) where rn < 11''')
                # sql = f'''select  stockcode,stockname,sum(changenum) totalnu from T_O_HK_BROKERHOLDNUM_NEW t where  t.tradeday = to_date({date}, 'yyyy/mm/dd') GROUP BY stockcode,stockname '''
                df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_changenum_list:
                    dflist.append(chunk)
                df_changenum = pd.concat(dflist)
                df_changenum.columns = ["tag", "stockcode", "stockname", "industryname", date]
                df_changenum.drop(columns=["tag"], inplace=True)
                df_changenum.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False,
                                       inplace=True)
                trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
                trade_df = trade_df[~trade_df.index.duplicated()]
            trade_df.fillna(0, inplace=True)
            # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            trade_df["近三天"] = trade_df.loc[:, self.tradedate_list[-3:]].apply(lambda x: x.sum(), axis=1)
            trade_df["近五天"] = trade_df.loc[:, self.tradedate_list[-5:]].apply(lambda x: x.sum(), axis=1)
            trade_df["近十天"] = trade_df.loc[:, self.tradedate_list[0:10]].apply(lambda x: x.sum(), axis=1)
            trade_df = trade_df.apply(lambda x: (x // 10000))
            trade_df.sort_values(by=self.tradedate_list, ascending=False, inplace=True)
            return trade_df
        except Exception as e:
            logger.error(e)
            return pd.DataFrame()

    def history_A_stock_rate(self, s):
        df = s.str.split(",", expand=True)
        df.columns = ["holdnum", "changenum"]
        df['holdnum'] = df['holdnum'].astype(np.float64)
        df['changenum'] = df['changenum'].astype(np.float64)
        return round(df['changenum'] / (df['holdnum'] - df['changenum']) * 100, 2)

    def history_A_stock_day_rate(self, x):
        df = x.str.split(",", expand=True)
        df.columns = ["holdnum", "changenum"]
        df['holdnum'] = df['holdnum'].astype(np.float64)
        df['changenum'] = df['changenum'].astype(np.float64)
        sto = df['changenum'].sum() / (df['holdnum'].iloc[-1] - df['changenum'].sum()) * 100 if df['holdnum'].iloc[-1] - \
                                                                                                df[
                                                                                                    'changenum'].sum() != 0 else 0
        return round(sto, 2)

    # 股票持仓变化率（%）
    def history_A_stock_lv(self) -> pd.DataFrame:
        """
        股票持仓变化率(%)
        """
        try:
            trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
            trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            for date in self.tradedate_list:
                sql = text(f"""select  stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) 
                when 'XD' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) 
                else stockname end stockname,industryname,sum(holdnum) ||','|| sum(changenum) AS text
                             from T_O_HK_BROKERHOLDNUM_NEW t where   tradeday = to_date({date}, 'yyyy/mm/dd')   group by stockcode,stockname,industryname""")
                df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_changenum_list:
                    dflist.append(chunk)
                df_changenum = pd.concat(dflist)
                df_changenum.columns = ["stockcode", "stockname", "industryname", date]
                df_changenum.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False,
                                       inplace=True)
                trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
            trade_df.fillna("1,0", inplace=True)
            # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            # trade_df = trade_df.apply(lambda x: (x / 10000).round(2))
            # print(trade_df)
            df = trade_df.apply(self.history_A_stock_rate)
            df['近三天'] = trade_df.loc[:, self.tradedate_list[-3:]].apply(self.history_A_stock_day_rate, axis=1)
            df['近五天'] = trade_df.loc[:, self.tradedate_list[-5:]].apply(self.history_A_stock_day_rate, axis=1)
            df['近十天'] = trade_df.loc[:, self.tradedate_list[0:10]].apply(self.history_A_stock_day_rate, axis=1)
            df.fillna(0, inplace=True)
            df = df.loc[(df.loc[:, self.tradedate_list[0:10]].sum(axis=1) != 0)]
            return df
        except Exception as e:
            return pd.DataFrame()

    def history_stock_holdnum_ktcap(self):
        """
        股票持仓相对流通股近10日变动率(%)
        """
        try:
            trade_df = pd.DataFrame(columns=["stockcode", "stockname", "industryname"])
            trade_df.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False, inplace=True)
            for date in self.tradedate_list:
                sql = text(f"""SELECT t.stockcode,t.stockname,t.industryname,t.sum_changenum ||','|| q.CIRCSKAMT ||',' || q.CIRCSKAMT * 10000 AS text 
                FROM (SELECT stockcode,case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_HK_BROKERHOLDNUM_NEW f where  
                f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) when 'XD' then (select distinct(stockname) 
                from T_O_HK_BROKERHOLDNUM_NEW f where  f.stockname not like 'XD%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,industryname,SUM(holdnum) AS sum_changenum  
                FROM T_O_HK_BROKERHOLDNUM_NEW t WHERE tradeday = to_date({date}, 'yyyy/mm/dd') GROUP BY stockcode,stockname,industryname) t, TQ_SK_SHARESTRUCHG q, TQ_SK_BASICINFO s 
                WHERE q.compcode =  s.compcode  AND t.stockcode=s.SYMBOL and  q.ENTRYDATE = ( select max(g.ENTRYDATE) from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO l 
                where g.compcode = l.compcode and l.symbol = s.symbol)  and  q.begindate = ( select max(g.begindate) from TQ_SK_SHARESTRUCHG g ,TQ_SK_BASICINFO l 
                where g.compcode = l.compcode and l.symbol = s.symbol) and t.sum_changenum != 0""")
                df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
                dflist = []
                for chunk in df_changenum_list:
                    dflist.append(chunk)
                df_changenum = pd.concat(dflist)
                df_changenum.columns = ["stockcode", "stockname", "industryname", date]
                df_changenum.set_index(["stockcode", 'stockname', "industryname"], drop=True, append=False,
                                       inplace=True)
                trade_df = pd.merge(trade_df, df_changenum, how='outer', left_index=True, right_index=True)
            trade_df.fillna("0,1,1", inplace=True)

            df = trade_df.apply(self.stock_ktcap_rate)
            # trade_df = trade_df.loc[(trade_df.sum(axis=1) != 0)]
            df.fillna(0, inplace=True)
            df = df.loc[(df.loc[:, self.tradedate_list[0:10]].sum(axis=1) != 0)]
            return df
        except:
            return pd.DataFrame()

    def history_stock_MARKETVALUE(self):
        """
        经纪商近10日持仓持仓市值(万)
        """
        try:
            tradedate_list = sorted(self.tradedate_list, reverse=True)
            tradedate_list = tuple(tradedate_list)
            sql = text(
                f"""select *  from (select to_char(t.tradeday,'yyyymmdd' ) tradeday,  t.brokername,  round(sum(t.MARKETVALUE) / 10000) changenum  from T_O_BrokerAllPosition t  where t.tradeday >= to_date({tradedate_list[-1]}, 'yyyy/mm/dd')  and t.changenum != 0  group by t.tradeday,  t.brokername ) PIVOT(max(changenum)  FOR tradeday IN {tradedate_list})""")
            df_changenum_list = pd.read_sql(sql, engine.connect(), chunksize=50000)
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

    def drop_duplicates_stockcode(self, df):
        df = df.copy()
        index_names = df.index.names
        df.reset_index(inplace=True)
        df.drop_duplicates(subset='stockcode', keep='last',
                           inplace=True)
        df.set_index(index_names, drop=True, inplace=True)
        return df

    def run_img_excel_sftp(self) -> None:
        try:
            fordate = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
            if len(self.tradedate_list) > 0:
                fordate = self.tradedate_list[-1]
            if len(self.tradedate_list) == 0:
                raise Exception("tradedate not exist")

            logger.info(self.start_time)
            logger.info(self.tradedate_list)
            logger.info(fordate)
            # ProcessPoolExecutor
            # ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=20) as pool:
                future1 = pool.submit(self.history_A_stock_top)
                future2 = pool.submit(self.history_A_stock_changenum)
                future4 = pool.submit(self.history_A_stock_ktcap)
                future5 = pool.submit(self.history_A_stock_industry)
                future6 = pool.submit(self.history_stock_broker)
                future7 = pool.submit(self.history_stock_ktcap)
                future8 = pool.submit(self.history_stock_industry)
                future9 = pool.submit(self.history_A_stock_lv)
                future12 = pool.submit(self.header.stock_header_description)
                future13 = pool.submit(self.history_stock_holdnum_ktcap)
                future14 = pool.submit(self.history_A_stock_ktcap_chicang)
                future15 = pool.submit(self.history_A_stock_ktcap_bankuai)
                future16 = pool.submit(self.history_A_stock_ktcap_xiaopiao)
                future17 = pool.submit(self.history_stock_MARKETVALUE)

                def get_result(des, future):
                    self.global_data_res[des] = future.result()

                future1.add_done_callback(functools.partial(get_result, "history_A_stock_top"))
                future2.add_done_callback(functools.partial(get_result, "history_A_stock_changenum"))
                future4.add_done_callback(functools.partial(get_result, "history_A_stock_ktcap"))
                future5.add_done_callback(functools.partial(get_result, "history_A_stock_industry"))
                future6.add_done_callback(functools.partial(get_result, "history_stock_broker"))
                future7.add_done_callback(functools.partial(get_result, "history_stock_ktcap"))
                future8.add_done_callback(functools.partial(get_result, "history_stock_industry"))
                future9.add_done_callback(functools.partial(get_result, "history_A_stock_lv"))
                future12.add_done_callback(functools.partial(get_result, "stock_header_description"))
                future13.add_done_callback(functools.partial(get_result, "history_stock_holdnum_ktcap"))
                future14.add_done_callback(functools.partial(get_result, "history_A_stock_ktcap_chicang"))
                future15.add_done_callback(functools.partial(get_result, "history_A_stock_ktcap_bankuai"))
                future16.add_done_callback(functools.partial(get_result, "history_A_stock_ktcap_xiaopiao"))
                future17.add_done_callback(functools.partial(get_result, "history_stock_MARKETVALUE"))

            with pd.ExcelWriter(self.bio, engine='xlsxwriter') as writer:
                des_df, stream_dict = self.global_data_res.get("stock_header_description", (pd.DataFrame(), None))
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
                history_A_stock_top_df = self.global_data_res.get("history_A_stock_top", pd.DataFrame())
                if not history_A_stock_top_df.empty:
                    history_A_stock_top_df.style.applymap(self.highlight_background,
                                                          subset=history_A_stock_top_df.columns[:]).to_excel(writer,
                                                                                                             sheet_name='股票至今十大买入卖出(万)',
                                                                                                             encoding="utf-8",
                                                                                                             merge_cells=False)
                history_A_stock_changenum_df = self.global_data_res.get("history_A_stock_changenum", pd.DataFrame())
                if not history_A_stock_changenum_df.empty:
                    history_A_stock_changenum_df = self.drop_duplicates_stockcode(history_A_stock_changenum_df)
                    history_A_stock_changenum_df.style.applymap(self.highlight_background,
                                                                subset=history_A_stock_changenum_df.columns[
                                                                       :]).to_excel(
                        writer,
                        sheet_name='股票近10日买入量(万股)',
                        encoding="utf-8",
                        merge_cells=False)
                history_A_stock_changenum_df_for3 = self.history_A_stock_changenum_for3(history_A_stock_changenum_df)
                if not history_A_stock_changenum_df_for3.empty:
                    history_A_stock_changenum_df_for3 = self.drop_duplicates_stockcode(
                        history_A_stock_changenum_df_for3)
                    history_A_stock_changenum_df_for3.style.applymap(self.highlight_background,
                                                                     subset=history_A_stock_changenum_df_for3.columns[
                                                                            :]).to_excel(
                        writer,
                        sheet_name='当日买入为前三日两倍以上(万股)',
                        encoding="utf-8",
                        merge_cells=False)
                history_A_stock_ktcap_chicang_df = self.global_data_res.get("history_A_stock_ktcap_chicang",
                                                                            pd.DataFrame())
                if not history_A_stock_ktcap_chicang_df.empty:
                    history_A_stock_ktcap_chicang_df = self.drop_duplicates_stockcode(history_A_stock_ktcap_chicang_df)
                    history_A_stock_ktcap_chicang_df.style.applymap(self.highlight_background,
                                                                    subset=history_A_stock_ktcap_chicang_df.columns[
                                                                           :]).to_excel(writer,
                                                                                        sheet_name='持仓(万)',
                                                                                        encoding="utf-8",
                                                                                        merge_cells=False,
                                                                                        engine='xlsxwriter')
                history_A_stock_ktcap_bankuai_df = self.global_data_res.get("history_A_stock_ktcap_bankuai",
                                                                            pd.DataFrame())
                if not history_A_stock_ktcap_bankuai_df.empty:
                    history_A_stock_ktcap_bankuai_df = self.drop_duplicates_stockcode(history_A_stock_ktcap_bankuai_df)
                    history_A_stock_ktcap_bankuai_df.style.applymap(self.highlight_background,
                                                                    subset=history_A_stock_ktcap_bankuai_df.columns[
                                                                           :]).to_excel(writer,
                                                                                        sheet_name='预测(万)',
                                                                                        encoding="utf-8",
                                                                                        merge_cells=False,
                                                                                        engine='xlsxwriter')
                history_A_stock_ktcap_xiaopiao_df = self.global_data_res.get("history_A_stock_ktcap_xiaopiao",
                                                                             pd.DataFrame())
                if not history_A_stock_ktcap_xiaopiao_df.empty:
                    history_A_stock_ktcap_xiaopiao_df = self.drop_duplicates_stockcode(
                        history_A_stock_ktcap_xiaopiao_df)
                    history_A_stock_ktcap_xiaopiao_df.style.applymap(self.highlight_background,
                                                                     subset=history_A_stock_ktcap_xiaopiao_df.columns[
                                                                            :]).to_excel(writer,
                                                                                         sheet_name='观察(万)',
                                                                                         encoding="utf-8",
                                                                                         merge_cells=False,
                                                                                         engine='xlsxwriter')

                history_A_stock_ktcap_df, trade_df_for5, trade_df_for5buy = self.global_data_res.get(
                    "history_A_stock_ktcap",
                    (
                        pd.DataFrame(),
                        pd.DataFrame(),
                        pd.DataFrame()))
                # history_A_stock_ktcap_df.style.background_gradient(subset=history_A_stock_ktcap_df.columns[:-3],
                # cmap="RdYlGn_r", high=0.2, low=0.1).applymap(highlight_background,
                # subset=history_A_stock_ktcap_df.columns[ -3:]).to_excel(writer, sheet_name='股票近10日买入额(万)',
                # encoding="utf-8", merge_cells=False, engine='xlsxwriter')
                if not history_A_stock_ktcap_df.empty:
                    history_A_stock_ktcap_df = self.drop_duplicates_stockcode(history_A_stock_ktcap_df)
                    history_A_stock_ktcap_df.style.applymap(self.highlight_background,
                                                            subset=history_A_stock_ktcap_df.columns[:]).to_excel(writer,
                                                                                                                 sheet_name='股票近10日买入额(万)',
                                                                                                                 encoding="utf-8",
                                                                                                                 merge_cells=False,
                                                                                                                 engine='xlsxwriter')
                if not trade_df_for5.empty:
                    trade_df_for5 = self.drop_duplicates_stockcode(trade_df_for5)
                    trade_df_for5.style.applymap(self.highlight_background,
                                                 subset=trade_df_for5.columns[:]).to_excel(writer,
                                                                                           sheet_name='股票近10日买入额前五买入(万)',
                                                                                           encoding="utf-8",
                                                                                           merge_cells=False,
                                                                                           engine='xlsxwriter')
                if not trade_df_for5buy.empty:
                    trade_df_for5buy = self.drop_duplicates_stockcode(trade_df_for5buy)
                    trade_df_for5buy.style.applymap(self.highlight_background,
                                                    subset=trade_df_for5buy.columns[:]).to_excel(writer,
                                                                                                 sheet_name='股票近10日买入额连续五日大于0(万)',
                                                                                                 encoding="utf-8",
                                                                                                 merge_cells=False,
                                                                                                 engine='xlsxwriter')

                history_A_stock_industry_df = self.global_data_res.get("history_A_stock_industry", pd.DataFrame())
                # history_A_stock_industry_df.style.background_gradient(subset=history_A_stock_industry_df.columns[:-3],
                # cmap="RdYlGn_r", high=0.2, low=0.1).applymap(highlight_background,
                # subset=history_A_stock_industry_df.columns[ -3:]).to_excel(writer, sheet_name='板块近10日买入额(万)',
                # encoding="utf-8", merge_cells=False, engine='xlsxwriter')
                if not history_A_stock_industry_df.empty:
                    history_A_stock_industry_df.style.applymap(self.highlight_background,
                                                               subset=history_A_stock_industry_df.columns[:]).to_excel(
                        writer,
                        sheet_name='板块近10日买入额(万)',
                        encoding="utf-8",
                        merge_cells=False,
                        engine='xlsxwriter')
                history_stock_broker_df = self.global_data_res.get("history_stock_broker", pd.DataFrame())
                if not history_stock_broker_df.empty:
                    history_stock_broker_df.style.applymap(self.highlight_background,
                                                           subset=history_stock_broker_df.columns[:]).to_excel(writer,
                                                                                                               sheet_name='经纪商近10日买入额(万)',
                                                                                                               encoding="utf-8",
                                                                                                               merge_cells=False,
                                                                                                               engine='xlsxwriter')
                history_stock_MARKETVALUE_df = self.global_data_res.get("history_stock_MARKETVALUE", pd.DataFrame())
                if not history_stock_MARKETVALUE_df.empty:
                    history_stock_MARKETVALUE_df.style.applymap(self.highlight_background,
                                                                subset=history_stock_MARKETVALUE_df.columns[
                                                                       :]).to_excel(
                        writer,
                        sheet_name='经纪商近10日持仓持仓市值(万)',
                        encoding="utf-8",
                        merge_cells=False,
                        engine='xlsxwriter')
                history_stock_ktcap_df = self.global_data_res.get("history_stock_ktcap", pd.DataFrame())
                if not history_stock_ktcap_df.empty:
                    history_stock_ktcap_df = self.drop_duplicates_stockcode(history_stock_ktcap_df)
                    history_stock_ktcap_df.style.applymap(self.highlight_background,
                                                          subset=history_stock_ktcap_df.columns[:]).to_excel(writer,
                                                                                                             sheet_name='股票买卖相对自由流通股近10日变动率(%)',
                                                                                                             encoding="utf-8",
                                                                                                             merge_cells=False,
                                                                                                             engine='xlsxwriter')
                history_stock_industry_df = self.global_data_res.get("history_stock_industry", pd.DataFrame())
                if not history_stock_industry_df.empty:
                    history_stock_industry_df.style.applymap(self.highlight_background,
                                                             subset=history_stock_industry_df.columns[:]).to_excel(
                        writer,
                        sheet_name='板块买卖相较于北上资金持有板块票总和近10日变动率(%)',
                        encoding="utf-8",
                        merge_cells=False,
                        engine='xlsxwriter')
                history_A_stock_lv_df = self.global_data_res.get("history_A_stock_lv", pd.DataFrame())
                if not history_A_stock_lv_df.empty:
                    history_A_stock_lv_df = self.drop_duplicates_stockcode(history_A_stock_lv_df)
                    history_A_stock_lv_df.style.applymap(self.highlight_background,
                                                         subset=history_A_stock_lv_df.columns[:]).to_excel(writer,
                                                                                                           sheet_name='股票买卖相对昨日持仓变化率(%)',
                                                                                                           encoding="utf-8",
                                                                                                           merge_cells=False,
                                                                                                           engine='xlsxwriter')
                history_stock_holdnum_ktcap_df = self.global_data_res.get("history_stock_holdnum_ktcap", pd.DataFrame())
                if not history_stock_holdnum_ktcap_df.empty:
                    history_stock_holdnum_ktcap_df = self.drop_duplicates_stockcode(history_stock_holdnum_ktcap_df)
                    history_stock_holdnum_ktcap_df.style.applymap(self.highlight_background,
                                                                  subset=history_stock_holdnum_ktcap_df.columns[
                                                                         :]).to_excel(writer,
                                                                                      sheet_name='股票近十日股票持仓相对流通股比例(%)',
                                                                                      encoding="utf-8",
                                                                                      merge_cells=False)

            # remote_path = "/home/guest/003-数据/001-经纪商数据/test"
            # remote_path = "/home/guest/tmp/excel"
            # remote_path = "/home/toptrade/file"
            logger.info(self.ssh_config.get("remote_path"))
            # self.vba_sftp_upload_xlsm(self.ssh_config.get("remote_path"), fordate)
            logger.info(f"Running time: Mins:{(time.time() - self.start_time) // 60} mins")
        except Exception as e:
            logger.error(e)
            traceback.print_exc()


def data_deal():
    '''
    future1 = pool.submit(self.history_A_stock_top)
    future2 = pool.submit(self.history_A_stock_changenum)
    future4 = pool.submit(self.history_A_stock_ktcap)
    future5 = pool.submit(self.history_A_stock_industry)
    future6 = pool.submit(self.history_stock_broker)
    future7 = pool.submit(self.history_stock_ktcap)
    future8 = pool.submit(self.history_stock_industry)
    future9 = pool.submit(self.history_A_stock_lv)
    future12 = pool.submit(self.header.stock_header_description)
    future13 = pool.submit(self.history_stock_holdnum_ktcap)
    future14 = pool.submit(self.history_A_stock_ktcap_chicang)
    future15 = pool.submit(self.history_A_stock_ktcap_bankuai)
    future16 = pool.submit(self.history_A_stock_ktcap_xiaopiao)
    future17 = pool.submit(self.history_stock_MARKETVALUE)
    '''
    tradedate = None
    hk = Hksftpdata(tradedate, auto=False)
    data = hk.history_A_stock_top()
    data.to_excel("./tmp/test02.xlsx", merge_cells=False)
    with pd.ExcelWriter("./tmp/test01.xlsx", engine='xlsxwriter') as writer:
        data.style.applymap(hk.highlight_background,
                            subset=data.columns[:]).to_excel(writer,
                                                             sheet_name='股票至今十大买入卖出(万)',
                                                             encoding="utf-8",
                                                             merge_cells=False)


if __name__ == "__main__":
    hk = Hksftpdata("20230413", auto=False)
    # hk = Hksftpdata(None, auto=True)
    hk.__str__()
    # data_deal()
