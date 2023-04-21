import sys
import os
import yaml

from sqlalchemy.orm.session import Session
from app.models.strategy_stock import HostInfo
from app.db.session import SessionLocal


def get_yaml_data(yaml_file):
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    data = yaml.load(file_data, Loader=yaml.Loader)
    return data


def fast_api_load_configuration():
    current_path = os.path.dirname(os.path.abspath(__file__))
    father_path = os.path.dirname(current_path)
    father_path = os.path.join(father_path, "conf")
    yaml_path = os.path.join(father_path, "config_fast_api.yaml")
    yaml_data = get_yaml_data(yaml_path)
    envs = yaml_data.get("envs")
    envs_info = yaml_data.get(envs, {})
    kafka_server = envs_info.get("kafka_server", {})
    return {"kafka_server": kafka_server}


def init_config(db: Session = SessionLocal(), hostname: str = None):
    if hostname:
        config = db.query(HostInfo).filter(
            HostInfo.hostname == hostname).first()
        return {'kafka_server': {'bootstrap_servers': [config.kafka_bootstrap_servers],
                                 'topics': {'command_topic': config.kafka_topic_command,
                                            'req_pipe': config.kafka_topic_req_pipe,
                                            'rsp_pipe': config.kafka_topic_rsp_pipe}},
                'twap_limit_count': config.twap_limit_count,
                'zmq_info': {'host': config.zmq_host, 'port': config.zmq_port},
                'pov_limit_count': config.pov_limit_count,
                'redis_info': {'host': config.redis_host, 'port': config.redis_port},
                'pov_limit_sub_count': config.pov_limit_sub_count}, config
    else:
        configs = db.query(HostInfo.redis_host, HostInfo.redis_port, HostInfo.kafka_bootstrap_servers,
                           HostInfo.kafka_topic_command, HostInfo.kafka_topic_req_pipe,
                           HostInfo.kafka_topic_rsp_pipe, HostInfo.twap_limit_count, HostInfo.pov_limit_count,
                           HostInfo.pov_limit_sub_count, HostInfo.zmq_host, HostInfo.zmq_port).all()
        return configs
