import time
from datetime import datetime

import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
import warnings
import os
from sqlalchemy import create_engine
import traceback
import cx_Oracle

warnings.filterwarnings("ignore")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
# engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=True)
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='dazh')  # dsn
engine = create_engine("oracle://fcdb:fcdb@" + dns, encoding='utf-8', echo=True)  # 建立ORM连接

def realtime_to_oracle(df: pd.DataFrame):
    if df is not None:
        df.to_sql("T_S_THS_INDUSTRY_TIMESHARE", engine, index=False, chunksize=500, if_exists='append')
