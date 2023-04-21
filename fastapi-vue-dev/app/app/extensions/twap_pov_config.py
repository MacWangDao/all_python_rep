import sys
import os
import yaml


def get_yaml_data(yaml_file):
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    data = yaml.load(file_data, Loader=yaml.Loader)
    return data


def twap_pov_load_configuration():
    current_path = os.path.dirname(os.path.abspath(__file__))
    father_path = os.path.dirname(current_path)
    father_path = os.path.join(father_path, "conf")
    yaml_path = os.path.join(father_path, "twap_pov_config.yaml")
    yaml_data = get_yaml_data(yaml_path)
    envs = yaml_data.get("envs")
    envs_info = yaml_data.get(envs, {})
    kafka_server = envs_info.get("kafka_server", {})
    user_info = envs_info.get("userinfo", {})
    twap_limit_count = yaml_data.get("twap_limit_count", 1)
    pov_limit_count = yaml_data.get("pov_limit_count", 1)
    pov_limit_sub_count = yaml_data.get("pov_limit_sub_count", 1)
    redis_info = envs_info.get("redis", {})
    zmq_info = envs_info.get("zmq", {})

    return {"kafka_server": kafka_server, "user_info": user_info, "twap_limit_count": twap_limit_count,
            "zmq_info": zmq_info,
            "pov_limit_count": pov_limit_count, "redis_info": redis_info, "pov_limit_sub_count": pov_limit_sub_count}
