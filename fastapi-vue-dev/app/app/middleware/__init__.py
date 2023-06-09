from fastapi import FastAPI

from app.extensions.config_fast_api import fast_api_load_configuration

from starlette.middleware.cors import CORSMiddleware

from app.core.config import settings
# from app.middleware.access_middle import AccessMiddleware
from starlette_context.middleware import ContextMiddleware
from starlette_context import plugins


def register_middleware(app: FastAPI):
    # midddleware fastapi是逆序注册的 所以最后注册RequestIdPlugin log reqeust_id 好让其他middleware使用
    # app.add_middleware(AccessMiddleware)

    app.add_middleware(ContextMiddleware, plugins=(plugins.RequestIdPlugin(),))

    if settings.BACKEND_CORS_ORIGINS:
        app.add_middleware(
            CORSMiddleware,
            # allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )


def register_yaml(app: FastAPI):
    config = fast_api_load_configuration()
    app.state.yaml_config = config
    print(f"yaml成功--->>{app.state.yaml_config}")



