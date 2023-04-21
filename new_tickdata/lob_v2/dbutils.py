# -*- coding: utf-8 -*-

import pandas as pd
import cx_Oracle
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError


'''
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
'''


class OracleWrapper():
    def __init__(self,usr,pwd,host,service,arraysize = 10000):
        self.engine = None
        try:
            engine = sqlalchemy.create_engine("oracle+cx_oracle://%s:%s@%s/?service_name=%s"%(usr,pwd,host,service), \
                                               arraysize=arraysize)
            self.engine = engine
        except SQLAlchemyError as e:
           print(e)
        
    def read(self,sql):
        try:
            df_orders = pd.read_sql(sql, self.engine)
            return df_orders
        except SQLAlchemyError as e:
           print(e)
'''
class InfluxWrapper():
    def __init__(self,url, token, org, bucket = None):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.async_result = None

        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, self.org=org)
        except e:
            print (e)

    def AddPoints(self,points, bucket = None, org = None):
        self.async_result = write_api.write(
            bucket if bucket is not None else self.bucket,
            org if org is not None else self.org, points)
'''


           
if __name__ == '__main__':
    db = OracleWrapper('pp','pp','10.9.0.22','pp')
    df = db.read('''SELECT * FROM ASHAREEODPRICES a WHERE S_YU_TRADEDATE = TO_DATE('2022-02-10','YYYY-MM-DD') AND S_YU_ASD_REC_ID LIKE '3_____.SZ' ''')
    
    print(df)