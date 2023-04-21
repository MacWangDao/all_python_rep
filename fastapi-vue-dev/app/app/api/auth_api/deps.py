import json
from typing import Generator, Optional

from aioredis import Redis
from fastapi import Depends, HTTPException, status
from jose import jwt, JWTError
from loguru import logger
from pydantic import BaseModel
from sqlalchemy.orm.session import Session

from app.core.auth import oauth2_scheme
from app.core.config import settings
from app.clients.email import MailGunConfig, EmailClient
from app.db.session import SessionLocal
from app.models.user import User, UserRole
from app.clients.reddit import RedditClient
from app import crud


class TokenData(BaseModel):
    userid: Optional[str] = None


def get_db() -> Generator:
    db = SessionLocal()
    db.current_user_id = None
    try:
        yield db
    finally:
        db.close()


def Singleton(cls):
    _instance = {}

    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]

    return _singleton


@Singleton
class RedisPool(object):
    def __init__(self, redis_5: Redis = None, redis_0: Redis = None):
        self.redis_5 = redis_5
        self.redis_0 = redis_0


def get_reddit_client() -> RedditClient:
    return RedditClient()


def get_email_client() -> EmailClient:
    config = MailGunConfig(
        API_KEY=settings.email.MAILGUN_API_KEY,
        DOMAIN_NAME=settings.email.MAILGUN_DOMAIN_NAME,
        BASE_URL=settings.email.MAILGUN_BASE_URL,
    )
    return EmailClient(config=config)


async def get_current_user(
        db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)
) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        token_plain = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.ALGORITHM],
            options={"verify_aud": False},
        )
        userid: str = token_plain.get("userid")
        if userid is None:
            raise credentials_exception
        token_data = TokenData(userid=userid)
    except JWTError:
        raise credentials_exception
    user = db.query(User).filter(User.userid == token_data.userid).first()
    if user is None:
        raise credentials_exception
    return user


def get_current_active_user(
        current_user: User = Depends(get_current_user),
) -> User:
    if not crud.user.is_active(current_user):
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


def get_current_active_superuser(
        current_user: User = Depends(get_current_user),
) -> User:
    if not crud.user.is_superuser(current_user):
        raise HTTPException(
            status_code=400, detail="The user doesn't have enough privileges"
        )
    return current_user


async def get_current_access_privileges(
        db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)
) -> User:
    credentials_exceptions = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        token_plain = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.ALGORITHM],
            options={"verify_aud": False},
        )
        userid: str = token_plain.get("userid")
        # roleid: str = token_plain.get("roleid")
        if userid is None:
            raise credentials_exceptions
        token_data = TokenData(userid=userid)
    except JWTError:
        raise credentials_exceptions
    try:
        redis_obj = RedisPool()
        redis_pool = redis_obj.redis_5
        if redis_pool is not None:
            userinfo = await redis_pool.hgetall(userid)
            if userinfo:
                username = userinfo.get("username")
                role_id = userinfo.get("role_id")
                access_token = userinfo.get("access_token")
                permissions = json.loads(userinfo.get("permissions", "[]"))
                if len(permissions) == 0:
                    user = db.query(User).filter(User.userid == token_data.userid).first()
                    perms = db.query(UserRole).filter(UserRole.rid == user.role_id).first()
                    if perms is None:
                        raise HTTPException(
                            status_code=status.HTTP_406_NOT_ACCEPTABLE,
                            detail="用户未添加角色登录",
                            headers={"WWW-Authenticate": "Bearer"})
                    perms_list = []
                    for p in perms.permission:
                        perms_list.append(p.ptype)
                    if len(perms_list) == 0:
                        raise HTTPException(
                            status_code=status.HTTP_406_NOT_ACCEPTABLE,
                            detail="用户未添加权限",
                            headers={"WWW-Authenticate": "Bearer"})
                    if user is None:
                        raise credentials_exceptions
                    setattr(user, 'permission', perms_list)
                    return user
                userid = userinfo.get("userid")
                lock = userinfo.get("lock")
                user = User()
                setattr(user, 'userid', int(userid))
                setattr(user, 'username', username)
                setattr(user, 'role_id', int(role_id))
                setattr(user, 'access_token', access_token)
                setattr(user, 'lock', lock)
                setattr(user, 'permission', permissions)
                return user
            else:
                user = db.query(User).filter(User.userid == token_data.userid).first()
                perms = db.query(UserRole).filter(UserRole.rid == user.role_id).first()
                if perms is None:
                    raise HTTPException(
                        status_code=status.HTTP_406_NOT_ACCEPTABLE,
                        detail="用户未添加角色登录",
                        headers={"WWW-Authenticate": "Bearer"})
                perms_list = []
                for p in perms.permission:
                    perms_list.append(p.ptype)
                if len(perms_list) == 0:
                    raise HTTPException(
                        status_code=status.HTTP_406_NOT_ACCEPTABLE,
                        detail="用户未添加权限",
                        headers={"WWW-Authenticate": "Bearer"})
                if user is None:
                    raise credentials_exceptions
                setattr(user, 'permission', perms_list)
                return user
    except Exception as e:
        logger.exception(e)
        user = db.query(User).filter(User.userid == token_data.userid).first()
        perms = db.query(UserRole).filter(UserRole.rid == user.role_id).first()
        if perms is None:
            raise HTTPException(
                status_code=status.HTTP_406_NOT_ACCEPTABLE,
                detail="用户未添加角色登录",
                headers={"WWW-Authenticate": "Bearer"})
        perms_list = []
        for p in perms.permission:
            perms_list.append(p.ptype)
        if len(perms_list) == 0:
            raise HTTPException(
                status_code=status.HTTP_406_NOT_ACCEPTABLE,
                detail="用户未添加权限",
                headers={"WWW-Authenticate": "Bearer"})
        if user is None:
            raise credentials_exceptions
        setattr(user, 'permission', perms_list)
        return user
    # user = db.query(User).filter(User.userid == token_data.userid).first()
    # perms = db.query(UserRole).filter(UserRole.rid == user.role_id).first()
    # if perms is None:
    #     raise HTTPException(
    #         status_code=status.HTTP_406_NOT_ACCEPTABLE,
    #         detail="用户未添加角色登录",
    #         headers={"WWW-Authenticate": "Bearer"})
    # perms_list = []
    # for p in perms.permission:
    #     perms_list.append(p.ptype)
    # if len(perms_list) == 0:
    #     raise HTTPException(
    #         status_code=status.HTTP_406_NOT_ACCEPTABLE,
    #         detail="用户未添加权限",
    #         headers={"WWW-Authenticate": "Bearer"})
    # if user is None:
    #     raise credentials_exceptions
    # setattr(user, 'permission', perms_list)
    # # if hasattr(user, 'permission'):
    # #     print(getattr(user, 'permission'))
    # return user


class LockTable:
    """
    权限管理的类型，把闭包改为类实现
    """

    def __init__(self):
        pass

    async def __call__(self, user: User = Depends(get_current_access_privileges)) -> bool:
        """
        生成的依赖函数
        """
        logger.info(user.username)
        if hasattr(user, "lock"):
            if getattr(user, "lock"):
                if int(getattr(user, "lock")) == 1:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="表格已锁定,无法编辑数据.",
                        headers={"WWW-Authenticate": "Bearer"})
        return True


class AccessPrivilegesPermissions:
    def __init__(self, permission: Optional[int], error_msg: Optional[str]):
        """
        传递需要验证是否具有的权限的列表。
        :param permission:
        """
        self.permission = permission
        self.error_msg = error_msg

    async def __call__(self, user: User = Depends(get_current_access_privileges)) -> User:
        """
        生成的依赖函数
        """
        if hasattr(user, "permission"):
            if self.permission not in getattr(user, "permission"):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"用户无{self.error_msg}权限",
                    headers={"WWW-Authenticate": "Bearer"})
        return user
