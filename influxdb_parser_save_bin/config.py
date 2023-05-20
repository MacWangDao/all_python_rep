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
    token = yaml_data.get("influxdb", {}).get("token")
    org = yaml_data.get("influxdb", {}).get("org")
    bucket = yaml_data.get("influxdb", {}).get("bucket")
    url = yaml_data.get("influxdb", {}).get("url")
    read_path = yaml_data.get("read_path")
    read_name = yaml_data.get("read_name")
    save_path = yaml_data.get("save_path")
    save_name = yaml_data.get("save_name")

    return {"read_path": read_path, "read_name": read_name, "save_path": save_path, "url": url, "save_name": save_name,
            "token": token,
            "org": org, "bucket": bucket}
