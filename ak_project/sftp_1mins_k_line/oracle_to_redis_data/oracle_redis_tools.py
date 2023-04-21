# import datetime
#
# import numpy as np
# import redis
# import pandas as pd
# import os
# from loguru import logger
# from sqlalchemy import create_engine, text
# import cx_Oracle
# import paramiko
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
# import concurrent.futures
#
# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# logger.add("oracle_redis_tools.log", rotation="100MB", encoding="utf-8", enqueue=True,
#            format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")
#
# dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='dazh')  # dsn
# engine = create_engine("oracle://fcdb:fcdb@" + dns, echo=False, pool_size=0,
#                        max_overflow=-1)
#
# pool_6 = redis.ConnectionPool(host='192.168.101.218', port=6379, encoding='utf-8',
#                               max_connections=1000, decode_responses=True, db=6)
#
#
# class SynchronizeData:
#     def __init__(self, keys):
#         self.keys = keys
#
#     def synch_ltdxdy_redis_start(self, start_date: str = None, enddate: str = None):
#         try:
#             self.keys.sort(reverse=True)
#             # ors = OracleRedis()
#             # ThreadPoolExecutor
#             # ProcessPoolExecutor
#             with ProcessPoolExecutor(max_workers=27) as executor:
#                 futures = []
#                 for tradedate in self.keys:
#                     if tradedate >= start_date and tradedate <= enddate:
#                         future = executor.submit(self._synch_ltdxdy_write_redis_data, tradedate)
#                         futures.append(future)
#                 concurrent.futures.wait(futures)
#         except Exception as e:
#             logger.exception(e)
#
#     def _synch_ltdxdy_write_redis_data(self, tradedate: str = None):
#         try:
#             # ors = OracleRedis()
#             redis_conn_6 = redis.Redis(connection_pool=pool_6)
#             sql = f"""
#                              SELECT n.SYMBOL, n.LCLOSE, n.tclose, n.tradedate, n.LTDXDY, n.NEGOTIABLEMV
#                                 FROM (
#                                          SELECT s.SYMBOL,
#                                                 q.LCLOSE,
#                                                 q.tclose,
#                                                 q.tradedate,
#                                                 NVL(x.LTDXDY, 1)                                                    LTDXDY,
#                                                 x.BEGINDATE,
#                                                 x.ENDDATE,
#                                                 REPLACE(x.ENDDATE, '19000101', TO_CHAR(trunc(sysdate), 'yyyymmdd')) new_enddate,
#                                                 q.NEGOTIABLEMV
#                                          FROM TQ_SK_BASICINFO s
#                                                   JOIN TQ_QT_SKDAILYPRICE q
#                                                        ON s.secode = q.secode AND q.tradedate = {tradedate} AND
#                                                           (SUBSTR(s.SYMBOL, 1, 1) = '0' OR SUBSTR(s.SYMBOL, 1, 1) = '3' OR SUBSTR(s.SYMBOL, 1, 1) = '6')
#                                                   LEFT JOIN TQ_SK_XDRY x ON s.secode = x.secode
#                                      ) n
#                                 WHERE NVL(n.BEGINDATE, {tradedate}) <= {tradedate}
#                                   AND NVL(n.new_enddate, {tradedate}) >= {tradedate}
#                             """
#             df_list = pd.read_sql(text(sql), engine.connect(), chunksize=30000)
#             dflist = []
#             for chunk in df_list:
#                 dflist.append(chunk)
#             df = pd.concat(dflist)
#             for date, row in df.iterrows():
#                 mapping = {"symbol": row["symbol"], "lclose": row["lclose"], "tclose": row["tclose"],
#                            "tradedate": row["tradedate"], "ltdxdy": row["ltdxdy"],
#                            "negotiablemv": row["negotiablemv"]}
#                 redis_conn_6.hset(f"xdy-{row['symbol']}-{row['tradedate']}", mapping=mapping)
#             logger.info(f"{tradedate},shape:{df.shape}")
#         except Exception as e:
#             logger.exception(e)
#
#
# def write_data_redis():
#     try:
#         pool = redis.ConnectionPool(host='192.168.101.218', port=6379, encoding='utf-8',
#                                     max_connections=1000, decode_responses=True, db=5)
#         redis_conn = redis.Redis(connection_pool=pool)
#         keys = redis_conn.keys()
#         sd = SynchronizeData(keys)
#         enddate = datetime.datetime.now().strftime("%Y%m%d")
#         sd.synch_ltdxdy_redis_start("20170103", enddate)
#     except Exception as e:
#         logger.exception(e)
#
#
# if __name__ == "__main__":
#     write_data_redis()
