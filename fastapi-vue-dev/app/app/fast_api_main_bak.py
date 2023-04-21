import os
import sys

import aioredis

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import uvicorn
from fastapi import FastAPI, WebSocket
from loguru import logger

from api.api import api_router
from middleware import register_yaml, register_middleware
from middleware.access_middle import SuppressNoResponseReturnedMiddleware
from extensions.twap_pov_config import twap_pov_load_configuration
from kafka_server.async_kafka_order_resp_server_v3 import AsyncKafkaServer
from app.extensions.config_fast_api import init_config

logger.add("log/fastapi-async_kafka_order_resp_server_v1.1.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")
app = FastAPI(title="北京泓佑私募",
              description="交易系统",
              version="0.1.1")


@app.on_event("startup")
async def startup():
    config = init_config(hostname="节点一")
    app.state.yaml_config = config
    logger.info(config)
    # app.state.yaml_config = twap_pov_load_configuration()
    # logger.info(f"yaml成功--->>{app.state.yaml_config}")
    app.state.redis_login = aioredis.from_url(f"redis://{app.state.yaml_config.get('redis_info', {}).get('host')}",
                                              encoding="utf-8",
                                              decode_responses=True, db=5)
    app.state.redis_info = app.state.yaml_config.get('redis_info', {})
    app.state.zmq_info = app.state.yaml_config.get('zmq_info', {})
    logger.info("app.state.redis_login:success")
    aks = AsyncKafkaServer(app.state.yaml_config)
    aks.run()
    logger.info("app.state.AsyncKafkaServer:success")
    app.state.aks = aks
    logger.info("startup")


@app.on_event("shutdown")
async def shutdown():
    logger.info("shutdown")


# register_yaml(app)
register_middleware(app)
app.add_middleware(SuppressNoResponseReturnedMiddleware)
app.include_router(api_router, prefix="")

if __name__ == "__main__":
    uvicorn.run("fast_api_main:app", host="192.168.101.218", port=8023, debug=True, reload=True)
