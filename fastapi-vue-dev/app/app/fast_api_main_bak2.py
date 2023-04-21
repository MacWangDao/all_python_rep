import os
import sys



sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import aioredis
from starlette import status
import uvicorn
from fastapi import FastAPI, Request, Response, HTTPException
from app.core import secret
from api.api import api_router
from middleware import register_middleware
from middleware.access_middle import SuppressNoResponseReturnedMiddleware
from kafka_server.async_kafka_order_resp_server_v3 import AsyncKafkaServer
from kafka_server.async_kafka_server_dban import AsyncKafkaServerMN
from app.extensions.config_fast_api import init_config
from app.api.auth_api.deps import RedisPool
from app.kafka_server.gateway import init_server
from app.extensions.logger import logger

app = FastAPI(title="北京泓佑私募",
              description="交易系统",
              version="0.1.1")


@app.on_event("startup")
async def startup():
    # init_server(app)
    # config = init_config(hostname="节点一")
    config, auth_config = init_config(hostname="节点二-测试")
    app.state.yaml_config = config
    app.state.auth_config = auth_config
    logger.info(config)
    app.state.redis_login = aioredis.from_url(f"redis://{app.state.yaml_config.get('redis_info', {}).get('host')}",
                                              encoding="utf-8",
                                              decode_responses=True, db=5)
    RedisPool(redis_5=app.state.redis_login)
    app.state.redis_info = app.state.yaml_config.get('redis_info', {})
    app.state.zmq_info = app.state.yaml_config.get('zmq_info', {})
    logger.info("app.state.redis_login:success")
    aks = AsyncKafkaServer(app.state.yaml_config)
    aks.run()

    logger.info("app.state.AsyncKafkaServer:success")
    app.state.aks = aks
    # ask_mn = AsyncKafkaServerMN(app.state.yaml_config)
    # ask_mn.run()
    # logger.info("AsyncKafkaServerMN:success")
    logger.info("startup")


@app.on_event("shutdown")
async def shutdown():
    logger.info("shutdown")


@app.middleware("http")
async def verify_token(request: Request, call_next):
    auth_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Bearer"},  # OAuth2的规范，如果认证失败，请求头中返回“WWW-Authenticate”
    )
    # 获取请求路径
    logger.info(f"X-Forwarded-For:{request.headers.get('x-forwarded-for')}")
    logger.info(f"X-Real-IP:{request.headers.get('x-real-ip')}")
    path: str = request.get('path')
    # 登录接口、docs文档依赖的接口，不做token校验
    if path.startswith('/bjhy/login') | path.startswith('/docs') | path.startswith('/openapi'):
        response = await call_next(request)
        return response
    else:
        response = Response(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},  # OAuth2的规范，如果认证失败，请求头中返回“WWW-Authenticate”
        )
        try:
            authorization: str = request.headers.get('authorization')
            if not authorization:
                return response
            token = authorization.split(' ')[1]
            token_pass = await secret.verify_token(token)
            if token_pass:
                logger.info("token验证通过")
                response = await call_next(request)
                return response
            else:
                return response
        except Exception as e:
            logger.error(e)
            return response
            # raise auth_error


register_middleware(app)
app.add_middleware(SuppressNoResponseReturnedMiddleware)

app.include_router(api_router, prefix="")

if __name__ == "__main__":
    uvicorn.run("fast_api_main:app", host="192.168.101.218", port=8023, debug=False, reload=False,
                log_config="uvicorn_config.json")
