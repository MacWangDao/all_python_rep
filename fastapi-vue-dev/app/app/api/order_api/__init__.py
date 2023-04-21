from fastapi import APIRouter
from app.api.order_api import order

order_router = APIRouter()
order_router.include_router(order.router, prefix="/bjhy", tags=["order"])
