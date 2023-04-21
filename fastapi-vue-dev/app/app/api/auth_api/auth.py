import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Response, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from loguru import logger
from sqlalchemy.orm.session import Session
from starlette.responses import RedirectResponse
from app import crud
from app import schemas
from app.api.auth_api import deps
from app.api.twap_pov_api.twap_pov_fast_api import parse_config, login_info
from app.clients.email import EmailClient
from app.core.auth import (
    authenticate,
    create_access_token,
)
from app.models.strategy_stock import AccountInfo
from app.models.user import User, UserRole
from app.core.email import send_registration_confirmed_email
from app.core.config import settings

router = APIRouter()


@router.post("/login", response_model=schemas.Msg, description="用户登录", summary="用户登录")
async def login(
        request: Request, response: Response, db: Session = Depends(deps.get_db),
        form_data: OAuth2PasswordRequestForm = Depends()
) -> Any:
    """
    Get the JWT for a user with data from OAuth2 request form body.
    """
    permission_status = 1
    user = authenticate(username=form_data.username, password=form_data.password, db=db)
    if not user:
        return {"code": 40000, "data": {"status": "fail"}, "message": "Incorrect username or password"}
    permissions = db.query(UserRole).filter(UserRole.rid == user.role_id).first()
    if permissions is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户未添加角色登录",
            headers={"WWW-Authenticate": "Bearer"})
    perms_list = []
    for p in permissions.permission:
        perms_list.append(p.ptype)
    setattr(user, 'permission', perms_list)
    if hasattr(user, 'permission'):
        if permission_status not in getattr(user, 'permission'):
            return {"code": 40000, "data": {"status": "fail"}, "message": "用户无登录权限"}
    # if not user:
    #     return {"code": 40000, "data": {"status": "fail"}, "message": "Incorrect username or password"}
    # raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(userid=str(user.userid), username=str(user.username), roleid=str(user.role_id),
                                       sub=str(user.userid))
    response.set_cookie("access_token", f"access_token:{access_token};username:{user.username};userid:{user.userid}")
    mapping = {
        "userid": user.userid,
        "username": user.username,
        "access_token": access_token,
        "role_id": user.role_id,
        "permissions": json.dumps(perms_list),
    }
    redis_login = request.app.state.redis_login
    await redis_login.hset(user.userid, mapping=mapping)
    uname = user.username
    userid = user.userid

    # accountinfos = db.query(AccountInfo.acid, AccountInfo.userid_id, AccountInfo.uname, AccountInfo.usernum,
    #                         AccountInfo.account, AccountInfo.password, AccountInfo.status, AccountInfo.description,
    #                         AccountInfo.ctime, AccountInfo.host_id).filter(
    #     AccountInfo.userid_id == userid,
    #     AccountInfo.uname == uname).all()
    accountinfos = db.query(AccountInfo).filter(
        AccountInfo.userid_id == userid,
        AccountInfo.uname == uname).all()
    if len(accountinfos) > 0:

        # config = parse_config(request)
        # producer = AIOKafkaProducer(bootstrap_servers=config.get("bootstrap_servers"),
        #                             key_serializer=lambda k: json.dumps(k).encode(),
        #                             value_serializer=lambda v: json.dumps(v).encode())
        try:
            # await producer.start()
            # request.app.state.aks.login_resp_list.clear()
            # auth_config = request.app.state.auth_config
            for account in accountinfos:
                if account.hostinfo.status:
                    aks = request.app.state.account_server.get(account.account)
                    aks.login_resp_list.clear()
                    account_login = login_info(account.hostinfo, account.usernum, account.account, account.password)
                    asyncio.ensure_future(aks.producer_client(user.username, account_login))
                # if auth_config.hostname == account.host_id:
                #     aks = request.app.state.account_server.get(account.account)
                #     aks.login_resp_list.clear()
                #     # account_login = login_info(auth_config, account.usernum, account.account, account.password)
                #     # asyncio.ensure_future(producer.send(config.get("req_pipe"), key=user.username, value=account_login))
                #     asyncio.ensure_future(aks.producer_client(user.username, account_login))
            await asyncio.sleep(1.7)
        except Exception as e:
            logger.exception(e)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            pass
            # await producer.stop()
    else:
        pass
        # request.app.state.aks.login_resp_list.clear()
    resp = {
        "access_token": access_token,
        "token_type": "bearer",
        "username": user.username,
        "userid": user.userid,
        # "login_resp_list": request.app.state.aks.login_resp_list,
        # "server_errorcode": request.app.state.aks.errorcode_res
    }
    logger.info(resp)
    return {"code": 20000, "data": resp, "message": "用户登录成功"}


@router.get("/me", response_model=schemas.Msg, description="token登录", summary="token登录")
async def read_users_me(request: Request, current_user: User = Depends(deps.get_current_access_privileges),
                        db: Session = Depends(deps.get_db)):
    # authorization: str = request.headers.get('authorization')
    """
    Fetch the current logged in user.
    """
    permission_status = 1
    user = current_user
    if hasattr(user, 'permission'):
        if permission_status not in getattr(user, 'permission'):
            return {"code": 40000, "data": {"status": "fail"}, "message": "用户无登录权限"}
    redis_login = request.app.state.redis_login
    access_token = await redis_login.hget(user.userid, "access_token")
    if access_token is None:
        return {"code": 40000, "data": {"status": "fail"}, "message": "Could not validate credentials"}
    delattr(user, 'userhashed_password')
    delattr(user, 'role_id')
    delattr(user, 'permission')
    delattr(user, 'userphonenum')
    delattr(user, 'useremail')
    uname = user.username
    userid = user.userid

    accountinfos = db.query(AccountInfo.acid, AccountInfo.userid_id, AccountInfo.uname, AccountInfo.usernum,
                            AccountInfo.account, AccountInfo.password, AccountInfo.status, AccountInfo.description,
                            AccountInfo.ctime).filter(AccountInfo.userid_id == userid,
                                                      AccountInfo.uname == uname).all()
    if len(accountinfos) > 0:
        config = parse_config(request)
        producer = AIOKafkaProducer(bootstrap_servers=config.get("bootstrap_servers"),
                                    key_serializer=lambda k: json.dumps(k).encode(),
                                    value_serializer=lambda v: json.dumps(v).encode())

        try:
            await producer.start()
            request.app.state.aks.login_resp_list.clear()
            for account in accountinfos:
                account_login = login_info(account.usernum, account.account, account.password)
                asyncio.ensure_future(producer.send(config.get("req_pipe"), key=user.username, value=account_login))
            await asyncio.sleep(1.2)
        except Exception as e:
            logger.error(e)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            await producer.stop()
    else:
        request.app.state.aks.login_resp_list.clear()
    setattr(user, 'login_resp_list', request.app.state.aks.login_resp_list)
    setattr(user, 'server_errorcode', request.app.state.aks.errorcode_res)
    logger.info(user)
    return {"code": 20000, "data": user, "message": "用户登录成功"}


@router.post("/signup", response_model=schemas.Msg, status_code=201, description="用户注册", summary="用户注册")
async def create_user_signup(
        *,
        db: Session = Depends(deps.get_db),
        email_client: EmailClient = Depends(deps.get_email_client),
        background_tasks: BackgroundTasks,
        user_in: schemas.user.UserCreate,
) -> Any:
    """
    Create new user without the need to be logged in.
    """

    user = db.query(User).filter(User.useremail == user_in.useremail).first()
    if user:
        raise HTTPException(
            status_code=400,
            detail="The user with this email already exists in the system",
        )
    user = crud.user.create(db=db, obj_in=user_in)

    # if settings.email.SEND_REGISTRATION_EMAILS:
    #     # Trigger email (asynchronous)
    #     background_tasks.add_task(
    #         send_registration_confirmed_email, user=user, client=email_client
    #     )
    return {"code": 20000, "data": user, "message": "用户注册成功"}


@router.get("/logout", response_model=schemas.Msg, description="退出登录", summary="退出登录")
async def logout(request: Request, response: Response, current_user: User = Depends(deps.get_current_user)):
    # response = RedirectResponse(url="/auth/login")
    redis_login = request.app.state.redis_login
    await redis_login.delete(current_user.userid)
    logger.info(f"{current_user.username}: delete redis_login")
    response.delete_cookie("access_token")
    return {"code": 20000, "data": {"status": "success"}, "message": "退出登录成功"}
    # return HTTPException(
    #     status_code=200,
    #     detail="logout system",
    # )
