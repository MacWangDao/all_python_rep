import asyncio
import os
import sys
from concurrent.futures import ProcessPoolExecutor

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from starlette import status
import uvicorn
from fastapi import FastAPI, Request, Response, HTTPException
from app.core import secret
from api.api import api_router
from middleware import register_middleware
from middleware.access_middle import SuppressNoResponseReturnedMiddleware
from app.kafka_server.gateway import init_server
from app.extensions.logger import logger
from app.redis.redis_oracel import pubsub_redis_start

app = FastAPI(title="北京泓佑私募",
              description="交易系统",
              version="0.1.1")


@app.on_event("startup")
async def startup():
    init_server(app)
    logger.info("startup")


@app.on_event("shutdown")
async def shutdown():
    logger.info("shutdown")


@app.middleware("http")
async def verify_token(request: Request, call_next):
    auth_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Bearer"},  # OAuth2的规范，如果认证失败，请求头中返"WWW-Authenticate"
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
            logger.exception(e)
            return response
            # raise auth_error


register_middleware(app)
app.add_middleware(SuppressNoResponseReturnedMiddleware)

app.include_router(api_router, prefix="")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    kw = {"host": "192.168.101.218", "port": 8023, "debug": False, "reload": False, "log_config": "uvicorn_config.json",
          "timeout_keep_alive": 60}
    with ProcessPoolExecutor(5) as executor:
        executor.submit(uvicorn.run, ("fast_api_main:app"), **kw)
        executor.submit(pubsub_redis_start)
    # uvicorn.run("fast_api_main:app", host="192.168.101.218", port=8023, debug=False, reload=False,
    #             log_config="uvicorn_config.json", timeout_keep_alive=60)
