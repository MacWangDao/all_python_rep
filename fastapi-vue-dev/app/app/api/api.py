from fastapi import APIRouter, Depends
from .auth_api import deps
# from .twap_pov_api import twap_pov_fast_api
# from .websocket_api import websocket_fast_api
# from .auth_api import auth
from . import auth_api
from . import twap_pov_api
from . import websocket_api
from . import order_api
from . import index_api

api_router = APIRouter()
# api_router.include_router(twap_pov_fast_api.router, prefix="/bjhy", tags=["twap_pov_fast_api"])
# api_router.include_router(websocket_fast_api.router, prefix="/bjhy", tags=["websocket_fast_api"])
# api_router.include_router(auth.router, prefix="/bjhy", tags=["auth"])
api_router.include_router(index_api.index_router)
api_router.include_router(auth_api.login_router)
# api_router.include_router(twap_pov_api.twap_pov_router, dependencies=[Depends(deps.get_current_active_user)])
api_router.include_router(twap_pov_api.twap_pov_router)
api_router.include_router(websocket_api.ws_router)
api_router.include_router(order_api.order_router)
