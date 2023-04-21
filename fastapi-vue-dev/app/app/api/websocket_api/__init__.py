from fastapi import APIRouter
from app.api.websocket_api import websocket_fast_api

ws_router = APIRouter()
ws_router.include_router(websocket_fast_api.router, prefix="/bjhy", tags=["websocket"])
