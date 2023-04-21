import sys
import os

import aioredis
from fastapi import FastAPI
from loguru import logger

from sqlalchemy.orm.session import Session

from app.api.auth_api.deps import RedisPool
from app.kafka_server.async_kafka_server_dban import AsyncKafkaServerMN
from app.models.strategy_stock import HostInfo
from app.db.session import SessionLocal
from app.kafka_server.async_kafka_order_resp_server_v3 import AsyncKafkaServer


def init_server(app: FastAPI, db: Session = SessionLocal(), hostname: str = None):
    try:
        account_server = {}
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
            config_list = db.query(HostInfo).filter(HostInfo.status == 1).all()
            redis_info = {}
            zmq_info = {}
            for config in config_list:
                server_info = {'kafka_server': {'bootstrap_servers': [config.kafka_bootstrap_servers],
                                                'topics': {'command_topic': config.kafka_topic_command,
                                                           'req_pipe': config.kafka_topic_req_pipe,
                                                           'rsp_pipe': config.kafka_topic_rsp_pipe}},
                               'twap_limit_count': config.twap_limit_count,
                               'zmq_info': {'host': config.zmq_host, 'port': config.zmq_port},
                               'pov_limit_count': config.pov_limit_count,
                               'redis_info': {'host': config.redis_host, 'port': config.redis_port},
                               'pov_limit_sub_count': config.pov_limit_sub_count}
                logger.info(config.hostname)
                if config.hostname == "节点三-挡板":
                    ask_mn = AsyncKafkaServerMN(server_info)
                    ask_mn.run()
                    logger.info("AsyncKafkaServerMN:success")
                    logger.info(config.kafka_bootstrap_servers)
                    for account in config.account:
                        account_server[account.account] = ask_mn
                else:
                    ask = AsyncKafkaServer(server_info)
                    ask.run()
                    logger.info("AsyncKafkaServer:success")
                    logger.info(config.kafka_bootstrap_servers)
                    for account in config.account:
                        account_server[account.account] = ask
                app.state.account_server = account_server
                redis_info["host"] = config.redis_host
                redis_info["port"] = config.redis_port
                zmq_info["host"] = config.zmq_host
                zmq_info["port"] = config.zmq_port
            app.state.redis_login = aioredis.from_url(f"redis://{redis_info.get('host')}",
                                                      encoding="utf-8",
                                                      decode_responses=True, db=5)
            app.state.redis_batid = aioredis.from_url(f"redis://{redis_info.get('host')}",
                                                      encoding="utf-8",
                                                      decode_responses=True, db=2)
            app.state.redis_query = aioredis.from_url(f"redis://{redis_info.get('host')}",
                                                      encoding="utf-8",
                                                      decode_responses=True, db=9)
            RedisPool(redis_5=app.state.redis_login)
            app.state.redis_info = redis_info
            app.state.zmq_info = zmq_info
            logger.info("app.state.redis_login:success")
    except Exception as e:
        logger.exception(e)
