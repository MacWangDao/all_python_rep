from jose import jwt, JWTError
from loguru import logger

from app.api.auth_api.deps import RedisPool
from app.core.config import settings


async def verify_token(token: str = None) -> bool:
    try:
        token_plain = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.ALGORITHM],
            options={"verify_aud": False},
        )
        userid: str = token_plain.get("userid")
        if userid is None:
            return False
    except JWTError:
        return False
    try:
        redis_obj = RedisPool()
        redis_pool = redis_obj.redis_5
        if redis_pool is not None:
            access_token = await redis_pool.hget(userid, "access_token")
            if not access_token:
                return False
            if token != access_token:
                return False
            return True
    except Exception as e:
        logger.exception(e)
        return False
