
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

# You can generate an API token from the "API Tokens Tab" in the UI

token = "njGCEN62vmioDQ-l86rmWGkZ0XDYdbQ168suv2W1FFKplBzZmD3WIrwPc6VVu9vvOvLLlB9d16RwB1zXJKNzcw=="
org = "org"
bucket = "quote-snapshot-3"

with InfluxDBClient(url="http://192.168.101.205:8086", token=token, org=org) as client:
    delete_api = client.delete_api()
    measurement = "ths_industry_index"
    start = "2022-08-07T00:00:00Z"
    stop = "2022-08-20T00:00:00Z"
    delete_api.delete(start, stop, f'_measurement="{measurement}"', bucket)
