from concurrent.futures.thread import ThreadPoolExecutor

import cx_Oracle as oracle
import os
import pandas as pd
import paramiko
from io import BytesIO
from sqlalchemy import create_engine

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dns = oracle.makedsn('192.168.101.215', 1521, service_name='dazh')  # dsn
engine = create_engine("oracle://fcdb:fcdb@" + dns, encoding='utf-8', echo=True)  # 建立ORM连接

data_set = {}
bro_set = {}


def get_tradedate_list(tradedate=None):
    tradedate_sql = "SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_BrokerAllPosition ORDER BY TRADEDAY desc) tr WHERE rownum<=10"
    if tradedate:
        tradedate_sql = f"SELECT to_char(tr.tradeday,'yyyymmdd') AS tradeday FROM (SELECT DISTINCT  tradeday FROM T_O_BrokerAllPosition ORDER BY TRADEDAY desc) tr WHERE tradeday <= to_date({tradedate}, 'yyyy/mm/dd') AND rownum<=10"
    df_tradedate = pd.read_sql(tradedate_sql, engine)
    tradedate_list = sorted(df_tradedate["tradeday"].tolist())
    return tradedate_list


def sftp_upload_xlsx(bio, remote_path, fordate, local_path):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname='192.168.101.211', username='toptrade', password='toptrade')
    stdin, stdout, stderr = ssh_client.exec_command(
        "echo 'toptrade' | sudo -S chmod 777 /home/guest/003-数据/001-经纪商数据 -R", timeout=300)
    out = stdout.readlines()
    ftp_client = ssh_client.open_sftp()
    xlsm = open(local_path + '/' + fordate + '全部持仓-all.xlsx', "wb")
    with ftp_client.open(remote_path + '/' + fordate + '全部持仓-all.xlsx', "w") as f:
        xlsm_byes = bio.getvalue()
        f.write(xlsm_byes)
        xlsm.write(xlsm_byes)


def get_history_all_stock(date):
    all_stock_sql = f"SELECT t.PROFITLOSSRATE,to_char(t.tradeday,'yyyy-mm-dd') AS tradeday,t.STOCKCODE,t.STOCKNAME,t.INDUSTRYCODE," \
                    f"t.INDUSTRYNAME,t.BROKERCODE,t.BROKERNAME,t.ISNEW,t.CHANGENUM,t.CONTINUOUSDAY,t.HOLDNUM,t.MARKETVALUE,t.PROFITLOSS," \
                    f"t.ALLAMOUNT,t.PERCOST,t.CHANGERATE,t.ALLCOST,t.HOLDRATE,t.HOLDTRENDIMGURL,t.MARKET,t.CHANGENUM*q.TCLOSE AS ch_tclose," \
                    f"q.TOPEN,q.TCLOSE,q.AVGPRICE,q.PCHG,q.VOL,q.AMOUNT,q.TURNRATE,q.NEGOTIABLEMV,q.TOTMKTCAP FROM T_O_BrokerAllPosition t," \
                    f"TQ_QT_SKDAILYPRICE q,TQ_SK_BASICINFO s where q.secode = s.secode AND t.stockcode=s.SYMBOL AND " \
                    f"q.tradedate={date} AND t.tradeday = to_date({date}, 'yyyy/mm/dd')"
    df_all_stock = pd.read_sql(all_stock_sql, engine, chunksize=50000)
    dflist = []
    for chunk in df_all_stock:
        dflist.append(chunk)
    df_all_stock = pd.concat(dflist)
    df_all_stock.columns = ["累计持仓盈亏比", "交易日期", "股票代码", "股票名称", "行业代码", "行业名称", "经纪商代码", "经纪商名称", "是否新开仓",
                            "当日买卖数量(万股)", "连续买卖天数", "持仓量(万股)", "持仓市值(万元)", "净收益额(万元)", "持仓成本(万元)",
                            "累计持仓成本(元/每股)", "变化比例", "平均单价(元)", "holdrate", "holdtrendimgurl", "market",
                            "当日买卖数额(万元)", "开盘价", "收盘价", "当日均价", "涨跌幅", "成交量(万股)",
                            "成交额(万元)", "换手率", "流通市值(万元)", "总市值(万元)"]
    df_all_stock["当日买卖数量(万股)"] = df_all_stock["当日买卖数量(万股)"].apply(lambda x: round(x / 10000))
    df_all_stock["持仓量(万股)"] = df_all_stock["持仓量(万股)"].apply(lambda x: round(x / 10000))
    df_all_stock["持仓市值(万元)"] = df_all_stock["持仓市值(万元)"].apply(lambda x: round(x / 10000))
    df_all_stock["净收益额(万元)"] = df_all_stock["净收益额(万元)"].apply(lambda x: round(x / 10000))
    df_all_stock["持仓成本(万元)"] = df_all_stock["持仓成本(万元)"].apply(lambda x: round(x / 10000))
    df_all_stock["当日买卖数额(万元)"] = df_all_stock["当日买卖数额(万元)"].apply(lambda x: round(x / 10000))
    df_all_stock["成交量(万股)"] = df_all_stock["成交量(万股)"].apply(lambda x: round(x / 10000))
    df_all_stock["成交额(万元)"] = df_all_stock["成交额(万元)"].apply(lambda x: round(x / 10000))
    df_all_stock["变化比例"] = df_all_stock["变化比例"].round(2)
    df_all_stock["换手率"] = df_all_stock["换手率"].round(2)
    df_all_stock["流通市值(万元)"] = df_all_stock["流通市值(万元)"].apply(lambda x: round(x / 10000))
    df_all_stock["总市值(万元)"] = df_all_stock["总市值(万元)"].apply(lambda x: round(x / 10000))
    data_set[date] = df_all_stock


def ten_days_stock_num(date):
    sql = f'''
               SELECT t.STOCKCODE,  case substr(stockname,1,2) when 'DR' then (select distinct(stockname) from T_O_BrokerAllPosition f WHERE 
               f.stockname not like 'DR%' and f.stockcode = t.stockcode and rownum = 1) 
               when 'XD' then (select distinct(stockname) from T_O_BrokerAllPosition f WHERE f.stockname not like 'XD%' AND
               f.stockcode = t.stockcode and rownum = 1) when 'TC' then (select distinct(stockname) from T_O_BrokerAllPosition f 
               WHERE f.stockname not like 'TC%' and f.stockcode = t.stockcode and rownum = 1) else stockname end stockname,  t.INDUSTRYCODE,  t.INDUSTRYNAME,
               t.BROKERCODE,  t.BROKERNAME,  round(t.CHANGENUM / 10000,2)  FROM T_O_BrokerAllPosition t  
               where t.tradeday = to_date({date}, 'yyyy/mm/dd')
               '''
    df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
    dflist = []
    for chunk in df_changenum_list:
        dflist.append(chunk)
    df_changenum = pd.concat(dflist)
    df_changenum.columns = ["stockcode", "stockname", "INDUSTRYCODE", "INDUSTRYNAME", "BROKERCODE",
                            "BROKERNAME", date]
    df_changenum.set_index(
        ["stockcode", "stockname", "INDUSTRYCODE", "INDUSTRYNAME", "BROKERCODE", "BROKERNAME"],
        drop=True,
        append=False, inplace=True)
    # df = pd.merge(df, df_changenum, how='outer', left_index=True, right_index=True)
    # df.fillna(0, inplace=True)
    bro_set[date] = df_changenum


def history_all_stock_to_df_add_dazh(tradedate_list):
    bio = BytesIO()

    with ThreadPoolExecutor(max_workers=23) as pool:
        for date in tradedate_list:
            future1 = pool.submit(get_history_all_stock, date)
            future2 = pool.submit(ten_days_stock_num, date)

    with pd.ExcelWriter(bio, engine='xlsxwriter') as writer:
        if data_set:
            sort_data = sorted(data_set.items(), key=lambda x: x[0])
            for k, v in sort_data:
                v.to_excel(writer,
                           sheet_name=k,
                           encoding="utf-8",
                           merge_cells=False, index=False)
        if bro_set:
            df = pd.DataFrame(
                columns=["stockcode", "stockname", "INDUSTRYCODE", "INDUSTRYNAME", "BROKERCODE", "BROKERNAME"])
            df.set_index(["stockcode", "stockname", "INDUSTRYCODE", "INDUSTRYNAME", "BROKERCODE", "BROKERNAME"],
                         drop=True,
                         append=False,
                         inplace=True)
            sort_bro = sorted(bro_set.items(), key=lambda x: x[0])
            for k, v in sort_bro:
                df = pd.merge(df, v, how='outer', left_index=True, right_index=True)
                df.fillna(0, inplace=True)
            # df.sort_values(by=df.columns[-1], ascending=False, inplace=True)
            df = df.loc[(df.sum(axis=1) != 0)]
            df.to_excel(writer,
                        sheet_name="十日买卖数量(万股)",
                        encoding="utf-8",
                        merge_cells=False)

    return bio


def run_all_stock(tradedate):
    tradedate_list = get_tradedate_list(tradedate)
    if len(tradedate_list) == 0:
        raise Exception("tradedate not exist")
    fordate = tradedate_list[-1]
    local_path = "./all_stock"
    # remote_path = "/home/guest/tmp"
    remote_path = "/home/guest/003-数据/001-经纪商数据"
    # remote_path = "/home/guest/003-数据/005-其他需求/每日板块开盘闭市涨幅排名"
    bio = history_all_stock_to_df_add_dazh(tradedate_list)
    sftp_upload_xlsx(bio, remote_path, fordate, local_path)


if __name__ == "__main__":
    tradedate = None
    run_all_stock(tradedate)
