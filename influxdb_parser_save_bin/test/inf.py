from typing import Generator

from influxdb_client import InfluxDBClient


def get_data_frame_stream(url=None, token=None, org=None, query=None) -> Generator:
    with InfluxDBClient(url=url, token=token, org=org, timeout=60_000) as client:
        df_stream_generator = client.query_api().query_data_frame_stream(query, org=org)
        return df_stream_generator


if __name__ == "__main__":
    pass
