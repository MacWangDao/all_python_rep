from io import BytesIO
from typing import List

import pandas as pd
from loguru import logger
from matplotlib import pyplot as plt, font_manager
from matplotlib.font_manager import FontProperties
from sqlalchemy import create_engine
import cx_Oracle as oracle


class Header(object):
    def __init__(self, ):

        self.buffer = BytesIO()
        dns = oracle.makedsn("192.168.101.215", 1521,
                             service_name="dazh")
        self.engine = create_engine(
            "oracle://fcdb:fcdb@" + dns,
            encoding='utf-8', echo=True)
        self.tradedate_list = self.get_tradedate_list_rownum()[:1]

    def img_stream(self, df_dict):
        try:
            font = FontProperties(
                fname=r"/home/toptrade/anaconda3/envs/selpy39/lib/python3.9/site-packages/matplotlib/mpl-data/fonts/ttf/SimHei.ttf",
                size=14)

            my_font = font_manager.FontProperties(
                fname=r"/home/toptrade/anaconda3/envs/selpy39/lib/python3.9/site-packages/matplotlib/mpl-data/fonts/ttf/SimHei.ttf")

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
                # tight_layout坐标轴重叠

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
            plt.tight_layout()
            plt.show()
            return stream_dict
        except Exception as e:
            logger.error(e)
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
                header_sum__sql = f'''
                    SELECT * FROM 
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose_60 FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND stockcode LIKE '60%' AND changenum > 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s0,
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose_00 FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND stockcode LIKE  '00%' AND changenum > 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s1,
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose_30 FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND stockcode LIKE '30%' AND changenum > 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s2,
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose_688 FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND stockcode LIKE '688%' AND changenum > 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s3,
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND changenum > 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s4
                    UNION ALL
                    SELECT * FROM 
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose_60_l FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND stockcode LIKE '60%' AND changenum < 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s0,
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose_00_l FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND stockcode LIKE  '00%' AND changenum < 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s1,
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose_30_l FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND stockcode LIKE '30%' AND changenum < 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s2,
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose_688_l FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND stockcode LIKE '688%' AND changenum < 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s3,
                    	(SELECT ROUND(SUM(t.sum_szb*q.tclose)/100000000)  AS sum_mul_tclose_l FROM (SELECT stockcode,stockname,sum(changenum) AS sum_szb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') AND changenum < 0 GROUP BY stockcode,stockname) t,
                    TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s WHERE q.secode = s.secode AND t.stockcode=s.SYMBOL AND q.tradedate={date}) s4
                    UNION All
                    SELECT * FROM 
                        (select count(1) AS hzb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode like '60%') c1,
                        (select count(1) AS szb  FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode like '00%') s2,
                        (select count(1) AS scy FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode like '30%') c3,
                        (select count(1) AS hkc  FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode like '688%') c4,
                        (select count(1) AS alsh FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) c5
                    UNION All
                    SELECT * FROM 
                        (select ROUND(sum(marketvalue)/100000000,0) AS hzb FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode like '60%') c1,
                        (select ROUND(sum(marketvalue)/100000000,0) AS szb  FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode like '00%') s2,
                        (select ROUND(sum(marketvalue)/100000000,0) AS scy FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode like '30%') c3,
                        (select ROUND(sum(marketvalue)/100000000,0) AS hkc  FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd') and stockcode like '688%') c4,
                        (select ROUND(sum(marketvalue)/100000000,0) AS alsh FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) c5
                    '''
                df_header = pd.read_sql(header_sum__sql, self.engine, chunksize=50000)
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

    def get_tradedate_list_rownum(self, tradedate: str = None) -> List[str]:
        """
        获取10个交易日
        """
        try:
            tradedate_sql = "SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_HK_BROKERHOLDNUM_NEW ORDER BY TRADEDAY desc) tr WHERE rownum<=10"
            if tradedate:
                tradedate_sql = f"SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_HK_BROKERHOLDNUM_NEW ORDER BY TRADEDAY desc) tr WHERE tradeday <= to_date({tradedate}, 'yyyy/mm/dd') AND rownum<=10"
            df_tradedate = pd.read_sql(tradedate_sql, self.engine)
            tradedate_list = sorted(df_tradedate["tradeday"].tolist())
            return tradedate_list
        except Exception as e:
            logger.error(e)


if __name__ == "__main__":
    h = Header()
    h.stock_header_description()
