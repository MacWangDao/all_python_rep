
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

# You can generate an API token from the "API Tokens Tab" in the UI

token = "pFGIYU6RQZ6bfObi0HD3B3KIF4xYq5IesSzvGVkSo675-6BPXk3phh0SUhVhi-_U93OBC1YhT_j-BQvwqNsDpQ=="
org = "bjhy"
bucket = "quote-v2"

with InfluxDBClient(url="http://192.168.101.201:8086", token=token, org=org) as client:
    delete_api = client.delete_api()
    measurement = "sp_ask_amt"
    start = "2022-11-03T09:00:00Z"
    stop = "2022-11-03T09:10:00Z"
    delete_api.delete(start, stop, f'_measurement="{measurement}"', bucket)
