from fastapi import APIRouter
from app.api.twap_pov_api import twap_pov_fast_api

twap_pov_router = APIRouter()
twap_pov_router.include_router(twap_pov_fast_api.router, prefix="/bjhy", tags=["twap_pov"])
