import sys
import os
import yaml


def get_yaml_data(yaml_file):
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    data = yaml.load(file_data, Loader=yaml.Loader)
    return data


def load_configuration():
    current_path = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(current_path, "config.yaml")
    yaml_data = get_yaml_data(yaml_path)
    instruments = yaml_data.get("instruments")
    x = yaml_data.get("matrix", {}).get("x")
    y = yaml_data.get("matrix", {}).get("y")
    token = yaml_data.get("influxdb", {}).get("token")
    org = yaml_data.get("influxdb", {}).get("org")
    bucket = yaml_data.get("influxdb", {}).get("bucket")
    url = yaml_data.get("influxdb", {}).get("url")
    time_interval = yaml_data.get("time_interval")
    timeRangeStart = yaml_data.get("timeRangeStart")
    timeRangeStop = yaml_data.get("timeRangeStop")
    out_path = yaml_data.get("out_path")
    sp_values = yaml_data.get("sp_values")
    db_config = {
        'user': yaml_data.get("oracle", {}).get("user"),
        'password': yaml_data.get("oracle", {}).get("password"),
        'host': yaml_data.get("oracle", {}).get("host"),
        'port': yaml_data.get("oracle", {}).get("port"),
        'service_name': yaml_data.get("oracle", {}).get("service_name")
    }
    return {"instruments": instruments, "x": x, "y": y, "url": url, "time_interval": time_interval, "token": token,
            "org": org, "bucket": bucket, "timeRangeStart": timeRangeStart, "timeRangeStop": timeRangeStop,
            "out_path": out_path, "sp_values": sp_values, "db_config": db_config}
