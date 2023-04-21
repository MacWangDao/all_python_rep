import time
from datetime import datetime

import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

# You can generate an API token from the "API Tokens Tab" in the UI
from ak_project.main.ths_index_industry import rank_index_industry_time_share_history_influxdb

token = "njGCEN62vmioDQ-l86rmWGkZ0XDYdbQ168suv2W1FFKplBzZmD3WIrwPc6VVu9vvOvLLlB9d16RwB1zXJKNzcw=="
org = "org"
bucket = "quote-snapshot-3"

with InfluxDBClient(url="http://192.168.101.205:8086", token=token, org=org) as client:
    # delete_api = client.delete_api()
    # delete_api.delete(bucket=bucket)
    write_api = client.write_api(write_options=ASYNCHRONOUS)
    mon = 10
    day_list = ["27", "28"]
    for day in day_list:
        date = pd.date_range(start=f'2022-{mon}-{day} 09:30:00', end=f'2022-{mon}-{day} 15:00:00', freq='min')
        rest_time = pd.date_range(start=f'2022-{mon}-{day} 11:30:00', end=f'2022-{mon}-{day} 12:59:00', freq='min')
        rest_time = rest_time.tolist()
        for date_time in date.tolist():
            if date_time not in rest_time:
                print(date_time)
                find_date = date_time.strftime("%Y%m%d %H:%M")
                df = rank_index_industry_time_share_history_influxdb(find_date=find_date)
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
                        GRADE = rec.get("GRADE")
                        RANK = rec.get("RANK")
                        ENTRYDATETIME = pd.to_datetime(date_time.strftime("%Y-%m-%d %H:%M:%S") + " +0800",
                                                       format="%Y-%m-%d %H:%M:%S %z", utc=True)
                        point = Point("ths_industry_index") \
                            .tag("INDLEVEL2CODE", INDLEVEL2CODE) \
                            .tag("INDLEVEL2NAME", INDLEVEL2NAME) \
                            .field("TIMESHAREPCHG", TIMESHAREPCHG) \
                            .field("TIMESHARECHANGE", TIMESHARECHANGE) \
                            .field("TIMESHAREVOL", TIMESHAREVOL) \
                            .field("TIMESHAREAMOUNT", TIMESHAREAMOUNT) \
                            .field("GRADE", TIMESHAREAMOUNT) \
                            .field("RANK", TIMESHAREAMOUNT) \
                            .time(ENTRYDATETIME, WritePrecision.S)
                        points.append(point)
                    write_api.write(bucket, org, points)
                time.sleep(5)
