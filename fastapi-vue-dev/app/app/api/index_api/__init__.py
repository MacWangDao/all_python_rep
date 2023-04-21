from fastapi import APIRouter
from app.api.index_api import index

index_router = APIRouter()
index_router.include_router(index.router, prefix="", tags=["index"])