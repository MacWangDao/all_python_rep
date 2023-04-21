import time
from datetime import datetime

import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from loguru import logger

token = "njGCEN62vmioDQ-l86rmWGkZ0XDYdbQ168suv2W1FFKplBzZmD3WIrwPc6VVu9vvOvLLlB9d16RwB1zXJKNzcw=="
org = "org"
bucket = "quote-snapshot-3"
client = InfluxDBClient(url="http://192.168.101.205:8086", token=token, org=org)

write_api = client.write_api(write_options=ASYNCHRONOUS)


def realtime_to_influxdb(df: pd.DataFrame):
    if df is not None:
        records = df.to_dict(orient="records")
        points = []
        for rec in records:
            INDLEVEL2CODE = rec.get("INDLEVEL2CODE")
            INDLEVEL2NAME = rec.get("INDLEVEL2NAME")
            TIMESHAREPCHG = rec.get("TIMESHAREPCHG")
            TIMESHARECHANGE = rec.get("TIMESHARECHANGE")
            TIMESHAREVOL = rec.get("TIMESHAREVOL")
            TIMESHAREAMOUNT = rec.get("TIMESHAREAMOUNT")
            ENTRYDATETIME = rec.get("ENTRYDATETIME")
            GRADE = rec.get("GRADE")
            RANK = rec.get("RANK")
            ENTRYDATETIME = pd.to_datetime(ENTRYDATETIME.strftime("%Y-%m-%d %H:%M:%S") + " +0800",
                                           format="%Y-%m-%d %H:%M:%S %z", utc=True)
            point = Point("ths_industry_index") \
                .tag("INDLEVEL2CODE", INDLEVEL2CODE) \
                .tag("INDLEVEL2NAME", INDLEVEL2NAME) \
                .field("TIMESHAREPCHG", TIMESHAREPCHG) \
                .field("TIMESHARECHANGE", TIMESHARECHANGE) \
                .field("TIMESHAREVOL", TIMESHAREVOL) \
                .field("TIMESHAREAMOUNT", TIMESHAREAMOUNT) \
                .field("GRADE", GRADE) \
                .field("RANK", RANK) \
                .time(ENTRYDATETIME, WritePrecision.S)
            points.append(point)
        write_api.write(bucket, org, points)
        logger.info("write_api.write")
