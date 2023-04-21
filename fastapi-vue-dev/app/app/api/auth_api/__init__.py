from fastapi import APIRouter
from . import auth

login_router = APIRouter()
login_router.include_router(auth.router, prefix="/bjhy", tags=["login"])
