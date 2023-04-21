from typing import Any

from fastapi import APIRouter

router = APIRouter()


@router.get("/", status_code=200, description="index", summary="index", include_in_schema=False)
async def index() -> Any:
    res = {
        "code": 20000,
        "data": {"status": 1},
        "message": "北京泓佑私募交易系统"

    }
    return res


@router.get('/favicon.ico', include_in_schema=False)
async def favicon() -> Any:

    return {}
