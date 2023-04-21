import datetime

import numpy as np
import redis
import pandas as pd
import os
from loguru import logger
from sqlalchemy import create_engine, text
import cx_Oracle
import paramiko
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import concurrent.futures

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
logger.add("ltdxdy_dailykline_parquet.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")
pool_5 = redis.ConnectionPool(host='192.168.101.218', port=6379, encoding='utf-8',
                              max_connections=1000, decode_responses=True, db=5)
pool_6 = redis.ConnectionPool(host='192.168.101.218', port=6379, encoding='utf-8',
                              max_connections=1000, decode_responses=True, db=6)
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='dazh')
engine = create_engine("oracle://fcdb:fcdb@" + dns, echo=False, pool_size=0,
                       max_overflow=-1)


class DailyKLineParquentFile(object):
    def __init__(self, stockcode: str = None, start_date: str = None, end_date: str = None,
                 in_path: str = None, out_path: str = None, file_type: str = ".csv"):
        self.host = '192.168.101.211'
        self.port = 22
        self.username = 'toptrade'
        self.password = 'toptrade'
        self.file_list = []
        self.stockcode = stockcode
        self.start_date = start_date
        self.end_date = end_date
        self.in_path = in_path
        self.out_path = out_path
        self.file_type = file_type
        self.get_read_all_file_path()
        self.redis_open = False

    def search_db_one_xdyon_data(self, sk_code: str = None, trade_date: str = None):
        sql = f"""
             SELECT * FROM  (
            SELECT s.SYMBOL, q.LCLOSE,q.tclose, q.tradedate, NVL(x.LTDXDY,1) LTDXDY,x.BEGINDATE,x.ENDDATE,
            REPLACE(x.ENDDATE,'19000101',TO_CHAR(trunc(sysdate),'yyyymmdd')) new_enddate,q.NEGOTIABLEMV
            FROM TQ_SK_BASICINFO s JOIN TQ_QT_SKDAILYPRICE q 
            ON s.secode=q.secode AND s.SYMBOL = {sk_code} AND q.tradedate = {trade_date} 
            LEFT JOIN TQ_SK_XDRY x ON s.secode=x.secode 
            ) n WHERE NVL(n.BEGINDATE,{trade_date})<={trade_date} AND NVL(n.new_enddate,{trade_date})>={trade_date}
        """
        df_list = pd.read_sql(text(sql), engine.connect(), chunksize=500)
        dflist = []
        for chunk in df_list:
            dflist.append(chunk)
        df = pd.concat(dflist)
        records = df.to_dict(orient="records")
        if len(records) > 0:
            return records[0]
        return {}

    def search_db_xdyon_cclose(self, sk_code: str = None, trade_date: str = None):
        pretrade_date = self.redis_conn_5.hget(trade_date, "pretrade_date")
        sql = f"""
         SELECT * FROM (
        SELECT s.SYMBOL ,s.SESNAME ,NVL(x.LTDXDY,1) LTDXDY ,x.BEGINDATE ,x.ENDDATE,REPLACE(x.ENDDATE,'19000101',TO_CHAR(trunc(sysdate),'yyyymmdd')) new_enddate  
        FROM TQ_SK_BASICINFO s LEFT JOIN TQ_SK_XDRY x ON s.secode=x.secode WHERE s.SYMBOL = {sk_code}
        ) n WHERE NVL(n.BEGINDATE,{pretrade_date})<={pretrade_date} AND NVL(n.new_enddate,{pretrade_date})>={pretrade_date}
        """
        df_list = pd.read_sql(text(sql), engine.connect(), chunksize=500)
        dflist = []
        for chunk in df_list:
            dflist.append(chunk)
        df = pd.concat(dflist)
        records = df.to_dict(orient="records")
        if len(records) > 0:
            return records[0]
        return {}

    def get_redis_daily_xdy(self, sk_code: str = None, trade_date: str = None):
        sk_name = f"xdy-{sk_code}-{trade_date}"
        mapping = self.redis_conn_6.hgetall(sk_name)
        return mapping

    def dkl_generate_parquet_file(self, csv_path: str = None):
        try:
            self.redis_conn_5 = redis.Redis(connection_pool=pool_5)
            self.redis_conn_6 = redis.Redis(connection_pool=pool_6)
            if csv_path is None:
                logger.info("csv_path is None")
                return
            if not os.path.exists(csv_path):
                logger.info("csv_path not exists")
                return
            (filepath, tempfilename) = os.path.split(csv_path)
            (filename, extension) = os.path.splitext(tempfilename)
            sk_code, ex = filename.split(".")
            stockcode = f"{ex}.{sk_code}"
            df = pd.read_csv(csv_path)
            df["trade_time"] = pd.to_datetime(df["trade_time"])
            grouped = df.groupby(df["trade_time"].dt.date)
            new_data = []
            index_list = []
            columns = []
            for date, group in grouped:
                trade_date = date.strftime("%Y%m%d")
                if self.redis_open:
                    mapping = self.get_redis_daily_xdy(sk_code=sk_code, trade_date=trade_date)
                    if len(mapping) == 0:
                        logger.error(f"{sk_code}-{trade_date}-redis data is empty")
                        continue
                    lclose = float(mapping.get("lclose"))
                    ltdxdy = float(mapping.get("ltdxdy"))
                    negotiablemv = float(mapping.get("negotiablemv")) * 10000
                    pretrade_date = self.redis_conn_5.hget(trade_date, "pretrade_date")
                    xdyon = self.get_redis_daily_xdy(sk_code=sk_code, trade_date=pretrade_date)
                    lclose_ltdxdy = float(xdyon.get("ltdxdy", 1))
                    lclose_div__xdy = ltdxdy * lclose / lclose_ltdxdy
                else:
                    xdyon_data = self.search_db_one_xdyon_data(sk_code=sk_code, trade_date=trade_date)
                    if len(xdyon_data) == 0:
                        logger.error(f"{sk_code}-{trade_date}-oracle data is empty")
                        continue
                    lclose = xdyon_data.get("lclose")
                    ltdxdy = xdyon_data.get("ltdxdy")
                    negotiablemv = xdyon_data.get("negotiablemv") * 10000
                    xdyon = self.search_db_xdyon_cclose(sk_code=sk_code, trade_date=trade_date)
                    lclose_ltdxdy = xdyon.get("ltdxdy", 1)
                    lclose_div__xdy = ltdxdy * lclose / lclose_ltdxdy
                group[["open", "high", "close", "low"]] = group[["open", "high", "close", "low"]].div(lclose_div__xdy)
                group["vol"] = group['vol'] / group['vol'].sum()
                group["amount"] = group["amount"].div(negotiablemv)
                cols = group[["open", "high", "close", "low", "vol", "amount"]].shape[0]
                if len(columns) == 0:
                    open_cols = [f"open/lclose-{str(i).zfill(3)}" for i in range(cols)]
                    columns.extend(open_cols)
                    high_cols = [f"high/lclose-{str(i).zfill(3)}" for i in range(cols)]
                    columns.extend(high_cols)
                    close_cols = [f"close/lclose-{str(i).zfill(3)}" for i in range(cols)]
                    columns.extend(close_cols)
                    low_cols = [f"low/lclose-{str(i).zfill(3)}" for i in range(cols)]
                    columns.extend(low_cols)
                    vol_cols = [f"vol/daily_sum-{str(i).zfill(3)}" for i in range(cols)]
                    columns.extend(vol_cols)
                    amount_cols = [f"amount/negotiablemv-{str(i).zfill(3)}" for i in range(cols)]
                    columns.extend(amount_cols)
                index_list.append((stockcode, pd.to_datetime(date)))
                new_data.append(
                    group[["open", "high", "close", "low", "vol", "amount"]].to_numpy(dtype=np.float32).flatten(
                        order="F"))
            index = pd.MultiIndex.from_tuples(index_list, names=["stockcode", "tradedate"])
            new_df = pd.DataFrame(new_data, columns=columns, index=index)
            if new_df.empty:
                return
            new_df.reset_index(inplace=True)
            out_filename = f"{stockcode}.parquet"
            if not self.out_path:
                self.out_path = "out_parquet"
                os.makedirs(self.out_path, exist_ok=True)
            else:
                os.makedirs(self.out_path, exist_ok=True)
            out_path_file = os.path.join(self.out_path, out_filename)
            logger.info(f"save:{out_path_file}")
            new_df.to_parquet(out_path_file, index=False)
        except Exception as e:
            logger.exception(e)

    def dkl_many_file_parquent(self):
        # ProcessPoolExecutor
        # ThreadPoolExecutor
        with ProcessPoolExecutor(max_workers=32) as executor:
            futures = []
            for csvfile in self.file_list:
                future = executor.submit(self.dkl_generate_parquet_file, csvfile)
                futures.append(future)
            concurrent.futures.wait(futures)

    def get_read_all_file_path(self, path: str = None):
        find_date: str = datetime.datetime.now().strftime("%Y-%m-%d")
        if self.in_path is None:
            return
        if not os.path.exists(self.in_path):
            logger.error(self.in_path)
        for root, dirs, files in os.walk(self.in_path):
            for file in files:
                (filename, extension) = os.path.splitext(file)
                if self.stockcode == filename.split(".")[0]:
                    if extension.lower() == self.file_type:
                        self.file_list.append(os.path.join(root, file))
                elif self.stockcode is None:
                    if extension.lower() == self.file_type:
                        self.file_list.append(os.path.join(root, file))
                else:
                    continue


def dxy_parquent_run():
    stockcode = None
    years = ["2017", "2018", "2019", "2020", "2021", "2022", "2023"]
    for year in years[:]:
        in_csv_path = f"/home/toptrade/project/ak_project/sftp_1mins_k_line/in_csv/orgin/{year}"
        out_parquent_path = f"/home/toptrade/project/ak_project/sftp_1mins_k_line/parquet/{year}"
        dkf = DailyKLineParquentFile(stockcode=stockcode, in_path=in_csv_path, out_path=out_parquent_path)
        dkf.redis_open = True
        dkf.dkl_many_file_parquent()


def merge_parquet(files, out_file):
    dfs = []
    for file in files:
        df = pd.read_parquet(file)
        dfs.append(df)
    if len(dfs) > 0:
        new_df = pd.concat(dfs)
        # new_df["tradedate"] = pd.to_datetime(new_df["tradedate"])
        # float64_column = new_df.select_dtypes(np.float64)
        # new_df[float64_column.columns] = float64_column.astype(np.float32)
        new_df.to_parquet(out_file, index=False)
        logger.info(f"count:{len(dfs)},shape:{new_df.shape}-{out_file}")


def many_merge_file_to_parquet():
    file_names = []
    paths = ["2017", "2018", "2019", "2020", "2021", "2022", "2023"]
    root_path = "/home/toptrade/project/ak_project/sftp_1mins_k_line/parquet"
    date = datetime.datetime.now().strftime("%Y%m%d")
    new_out_path = os.path.join(root_path, f"parquet_{date}")
    os.makedirs(new_out_path, exist_ok=True)
    for path in paths:
        out_path = os.path.join(root_path, path)
        for root, dirs, files in os.walk(out_path):
            for file in files:
                file_names.append(file)
    file_names = list(set(file_names))
    paths.sort()
    merge_path = []
    for filename in file_names:
        out_files = []
        for path in paths:
            file_path = os.path.join(os.path.join(root_path, path), filename)
            if os.path.exists(file_path):
                out_files.append(file_path)
        if len(out_files) > 0:
            out_file = os.path.join(new_out_path, filename)
            merge_path.append(
                (out_files, out_file)
            )
    with ProcessPoolExecutor(max_workers=32) as executor:
        futures = []
        for out_files, out_file in merge_path:
            future = executor.submit(merge_parquet, out_files, out_file)
            futures.append(future)
        concurrent.futures.wait(futures)


class SynchronizeData:
    def __init__(self, keys):
        self.keys = keys

    def synch_ltdxdy_redis_start(self, start_date: str = None, enddate: str = None):
        try:
            self.keys.sort(reverse=True)
            # ThreadPoolExecutor
            # ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=27) as executor:
                futures = []
                for tradedate in self.keys:
                    if tradedate >= start_date and tradedate <= enddate:
                        future = executor.submit(self._synch_ltdxdy_write_redis_data, tradedate)
                        futures.append(future)
                concurrent.futures.wait(futures)
        except Exception as e:
            logger.exception(e)

    def _synch_ltdxdy_write_redis_data(self, tradedate: str = None):
        try:
            redis_conn_6 = redis.Redis(connection_pool=pool_6)
            sql = f"""
                             SELECT n.SYMBOL, n.LCLOSE, n.tclose, n.tradedate, n.LTDXDY, n.NEGOTIABLEMV
                                FROM (
                                         SELECT s.SYMBOL,
                                                q.LCLOSE,
                                                q.tclose,
                                                q.tradedate,
                                                NVL(x.LTDXDY, 1)                                                    LTDXDY,
                                                x.BEGINDATE,
                                                x.ENDDATE,
                                                REPLACE(x.ENDDATE, '19000101', TO_CHAR(trunc(sysdate), 'yyyymmdd')) new_enddate,
                                                q.NEGOTIABLEMV
                                         FROM TQ_SK_BASICINFO s
                                                  JOIN TQ_QT_SKDAILYPRICE q
                                                       ON s.secode = q.secode AND q.tradedate = {tradedate} AND
                                                          (SUBSTR(s.SYMBOL, 1, 1) = '0' OR SUBSTR(s.SYMBOL, 1, 1) = '3' OR SUBSTR(s.SYMBOL, 1, 1) = '6')
                                                  LEFT JOIN TQ_SK_XDRY x ON s.secode = x.secode
                                     ) n
                                WHERE NVL(n.BEGINDATE, {tradedate}) <= {tradedate}
                                  AND NVL(n.new_enddate, {tradedate}) >= {tradedate}
                            """
            df_list = pd.read_sql(text(sql), engine.connect(), chunksize=30000)
            dflist = []
            for chunk in df_list:
                dflist.append(chunk)
            df = pd.concat(dflist)
            for date, row in df.iterrows():
                mapping = {"symbol": row["symbol"], "lclose": row["lclose"], "tclose": row["tclose"],
                           "tradedate": row["tradedate"], "ltdxdy": row["ltdxdy"],
                           "negotiablemv": row["negotiablemv"]}
                redis_conn_6.hset(f"xdy-{row['symbol']}-{row['tradedate']}", mapping=mapping)
            logger.info(f"{tradedate},shape:{df.shape}")
        except Exception as e:
            logger.exception(e)


def write_data_redis():
    try:
        redis_conn = redis.Redis(connection_pool=pool_5)
        keys = redis_conn.keys()
        sd = SynchronizeData(keys)
        enddate = datetime.datetime.now().strftime("%Y%m%d")
        sd.synch_ltdxdy_redis_start("20170103", enddate)
    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    """
    ps -aux | grep sftp_ltdxdy_dailykline_redis_tools_process_new.py | grep -v grep |awk '{print $2}'| xargs kill -9
    ps -aux | grep sftp_ltdxdy_dailykline_redis_tools_process_new.py | awk '{print $2}'|xargs kill -9
    scp -P 12220 -r ./parquet_20230420 percy@cronnex.com:/home/percy/works/hongyou/quote_data/quote_daily
    3237587lzj
    """
    write_data_redis()
    dxy_parquent_run()
    many_merge_file_to_parquet()
