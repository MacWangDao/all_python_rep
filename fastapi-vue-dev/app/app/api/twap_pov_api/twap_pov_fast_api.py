import asyncio
import itertools
import json
import traceback

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, Body, Request, Form, Depends, HTTPException
from loguru import logger
from starlette.responses import RedirectResponse

from app import schemas
from app.command_kafka.send_command_service import service
from app.api.auth_api import deps
from app.models.strategy_stock import AccountInfo
from app.models.user import User
from sqlalchemy.orm.session import Session

router = APIRouter()
command_id_counter = itertools.count()


# await producer.start()
# @router.post("/trade", response_model=Response)
# async def twap_pov_base(resp: Response):
#     print(resp.form1Data)
#     print(resp.form2Data)
#     print(resp.form3Data)
#     print(resp.form4Data)
#     return {"code": 20000, "data": {"response_model": True}, "message": "成功"}


@router.post("/trade", description="json格式下单", summary="json格式下单")
async def twap_pov_base(request: Request, body: dict = Body()):
    logger.info(body)
    stockcode = ""
    command_list = parse_body(body)
    if isinstance(command_list, str):
        return {"code": 20040, "data": {"response_model": True, "res": command_list}, "message": "失败"}
    elif isinstance(command_list, list):
        if not command_list:
            return {"code": 20041, "data": {"response_model": True, "stockcode": stockcode},
                    "message": "失败,TWAP or PoV 未选择!!!"}
        for command in command_list:
            service(request.app.state.yaml_config, command)
            stockcode = command.get("stockcode")

    return {"code": 20000, "data": {"response_model": True, "stockcode": stockcode}, "message": "成功"}


def parse_body(body):
    command_list = []
    body_form1 = body.get("form1Data", {})
    body_form2 = body.get("form2Data", {})
    body_form3 = body.get("form3Data", {})
    body_form4 = body.get("form4Data", {})
    stockcode = body_form1.get("stockCode", {})
    price = None
    if not stockcode:
        return "stockcode is empty"
    bsdir = body_form1.get("bsdir")
    if bsdir == 1:
        price = body_form1.get("open_price")
    elif bsdir == 2:
        price = body_form1.get("sell_price")
    if price == 0:
        return "price is empty"
    if price is not None:
        price = float(price)
    exchangeid = body_form1.get("market")
    vol = body_form1.get("salesVolume")
    if not vol:
        return "vol is empty"
    vol = int(vol)
    if body_form2:
        period = 0
        step = 0
        cancel = 0
        type = 2
        pov_check_group = body_form2.get("pov_check_group", [])
        limit = 0
        if "buy" in pov_check_group or "sell" in pov_check_group:
            limit = 1
        if bsdir == 1:
            period = body_form2.get("BuyingTransactionRate")
            step = body_form2.get("buyStep")
            cancel = body_form2.get("purchaseCancellationTime")
        elif bsdir == 2:
            period = body_form2.get("SellingTransactionRate")
            step = body_form2.get("sellStep")
            cancel = body_form2.get("salesCancellationTime")
        if period == 0 or step == 0 or cancel == 0:
            return "period step cancel is empty"
        period = int(period)
        step = int(step)
        cancel = int(cancel)
        command_list.append({
            "id": next(command_id_counter),
            "vol": vol, "period": period, "step": step, "cancel": cancel, "type": type, "stockcode": stockcode,
            "price": price, "bsdir": bsdir, "exchangeid": exchangeid, "limit": limit
        })
    if body_form3:
        period = 0
        step = 0
        cancel = 0
        check_group = body_form3.get("check_group", [])
        limit = 0
        if "bestPriceCounterparty" in check_group and "limitComplete" in check_group:
            limit = 1
        type = 1
        if bsdir == 1:
            period = body_form3.get("buyingCycle")
            step = body_form3.get("buyStep")
            cancel = body_form3.get("purchaseCancellationTime")
        elif bsdir == 2:
            period = body_form3.get("sellingCycle")
            step = body_form3.get("sellStep")
            cancel = body_form3.get("salesCancellationTime")
        if period == 0 or step == 0 or cancel == 0:
            return "period step cancel is empty"
        period = int(period)
        step = int(step)
        cancel = int(cancel)
        command_list.append({
            "id": next(command_id_counter),
            "vol": vol, "period": period, "step": step, "cancel": cancel, "type": type, "stockcode": stockcode,
            "price": price, "bsdir": bsdir, "exchangeid": exchangeid, "limit": limit
        })
    if body_form4:
        pass
    return command_list


def send_msg(type=None, securityid="", exchangeid=1, account=None):
    order = {"type": type, "reqid": 2,
             "accountInfo": {'account': account}, "actionid": 1, "batchno": 1,
             "securityid": securityid, "exchangeid": exchangeid}
    return order


@router.get("/query", response_model=schemas.Msg, description="qtype=13查询订单,9查询持仓,11查询资金,15查询成交",
            summary="qtype=13查询订单,9查询持仓,11查询资金,15查询成交")
async def twap_pov_query(request: Request, qtype: int = 13, account: str = None, securityid: str = "",
                         orderno: str = "",
                         sessionid: int = 0, current_user: User = Depends(
            deps.AccessPrivilegesPermissions(permission=2, error_msg="查询订单"))):
    logger.info("query")
    logger.info(current_user.username)
    if not account:
        return {"code": 40000, "data": {"status": "fail"}, "message": "账号不能为空"}
    # bootstrap_servers = request.app.state.yaml_config.get("kafka_server", {}).get("bootstrap_servers", [])
    # req_pipe = request.app.state.yaml_config.get("kafka_server", {}).get("topics", {}).get("req_pipe")
    aks = request.app.state.account_server.get(account)
    if aks is None:
        return {"code": 40000, "data": {"status": "fail"}, "message": "下单失败,交易系统未启动."}
    # accountinfo = db.query(AccountInfo.host_id).filter(AccountInfo.userid_id == current_user.userid,
    #                                                    AccountInfo.account == account).first()
    # auth_config = request.app.state.auth_config
    # if auth_config.hostname != accountinfo.host_id:
    #     return {"code": 40000, "data": {"status": "fail"}, "message": "账号无数据"}
    # producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers,
    #                             key_serializer=lambda k: json.dumps(k).encode(),
    #                             value_serializer=lambda v: json.dumps(v).encode())
    try:
        # await producer.start()
        aks.query_order.clear()
        aks.hold_stock.clear()
        aks.total_asset.clear()
        aks.success_order.clear()
        redis_query = request.app.state.redis_query
        keys = await redis_query.keys()
        orderno_list = []
        if qtype == 13:
            query_order = send_msg(type=13, securityid=securityid, account=account)
            query_order["orderno"] = orderno
            query_order["sessionid"] = sessionid
            asyncio.ensure_future(aks.producer_client("user-query", query_order))
            await asyncio.sleep(1)
            if 12 not in current_user.permission:
                for key in keys:
                    userid_orderno = await redis_query.hmget(key, "userid", "orderno")
                    if len(userid_orderno) == 2:
                        if userid_orderno[0] is not None and int(userid_orderno[0]) == current_user.userid:
                            orderno_list.append(userid_orderno[1])
                orderinfo_list = []
                for order in aks.query_order.get("orderInfo", []):
                    if order.get("orderno") in orderno_list:
                        orderinfo_list.append(order)

                aks.query_order.update({"orderInfo": orderinfo_list})

            return {"code": 20000, "data": aks.query_order, "message": "订单查询"}

        elif qtype == 9:
            query_order = send_msg(type=9, exchangeid=0, securityid=securityid, account=account)
            asyncio.ensure_future(aks.producer_client("user-hold", query_order))
            # asyncio.ensure_future(producer.send(req_pipe, key="user-hold", value=query_order))
            await asyncio.sleep(1)
            return {"code": 20000, "data": aks.hold_stock, "message": "持仓查询"}
        elif qtype == 11:
            query_order = send_msg(type=11, account=account)
            asyncio.ensure_future(aks.producer_client("user-asset", query_order))
            # asyncio.ensure_future(producer.send(req_pipe, key="user-asset", value=query_order))
            await asyncio.sleep(1)
            return {"code": 20000, "data": aks.total_asset, "message": "金额查询"}
        elif qtype == 15:
            query_order = send_msg(type=15, account=account)
            asyncio.ensure_future(aks.producer_client("user-success", query_order))
            # asyncio.ensure_future(producer.send(req_pipe, key="user-success", value=query_order))
            await asyncio.sleep(1)
            if 12 not in current_user.permission:
                for key in keys:
                    userid_orderno = await redis_query.hmget(key, "userid", "orderno")
                    if len(userid_orderno) == 2:
                        if userid_orderno[0] is not None and int(userid_orderno[0]) == current_user.userid:
                            orderno_list.append(userid_orderno[1])
                orderinfo_list = []
                for order in aks.success_order.get("tradeInfo", []):
                    if order.get("orderno") in orderno_list:
                        orderinfo_list.append(order)
                aks.success_order.update({"tradeInfo": orderinfo_list})
            return {"code": 20000, "data": aks.success_order, "message": "成交查询"}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        pass
        # await producer.stop()


@router.post("/cancel", response_model=schemas.Msg, description="撤单,ctype=0默认撤所有订单", summary="撤单,ctype=0默认撤所有订单")
async def twap_pov_cancal(request: Request, cancels: schemas.combine.CancelOrderList, current_user: User = Depends(
    deps.AccessPrivilegesPermissions(permission=2, error_msg="撤单")), ):
    logger.info("cancel")
    logger.info(current_user.username)
    if not cancels.account:
        return {"code": 40000, "data": {"status": "fail"}, "message": "账号不能为空"}
    aks = request.app.state.account_server.get(cancels.account)
    if aks is None:
        return {"code": 40000, "data": {"status": "fail"}, "message": "下单失败,交易系统未启动."}
    # bootstrap_servers = request.app.state.yaml_config.get("kafka_server", {}).get("bootstrap_servers", [])
    # req_pipe = request.app.state.yaml_config.get("kafka_server", {}).get("topics", {}).get("req_pipe")
    # producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers,
    #                             key_serializer=lambda k: json.dumps(k).encode(),
    #                             value_serializer=lambda v: json.dumps(v).encode())
    try:
        # await producer.start()
        # request.app.state.aks.query_order.clear()
        aks.query_order.clear()
        if cancels.ctype:
            query_order = send_msg(type=13, securityid="", account=cancels.account)
            asyncio.ensure_future(aks.producer_client("user-query", query_order))
            await asyncio.sleep(1)
            orderinfo = aks.query_order.get("orderInfo", [])
            for order in orderinfo:
                orderstatus = order.get("orderstatus")
                orderno = order.get("orderno")
                sessionid = order.get("sessionid")
                securityid = order.get("securityid")
                if orderstatus in [0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13, 14]:
                    cancel_order = send_msg(type=7, securityid=securityid, account=cancels.account)
                    cancel_order["orderno"] = orderno
                    cancel_order["sessionid"] = sessionid
                    logger.info(cancel_order)
                    asyncio.ensure_future(aks.producer_client("cancel_order", cancel_order))
                    # asyncio.ensure_future(producer.send(req_pipe, key="cancal_order", value=cancel_order))
                else:
                    logger.warning("It is no order cancel !!!")
        else:
            cancal_list = cancels.cancel_list
            for cancel in cancal_list:
                cancel_order = send_msg(type=7, securityid=cancel.securityid, account=cancels.account)
                cancel_order["orderno"] = cancel.orderno
                cancel_order["sessionid"] = cancel.sessionid
                logger.info(cancel_order)
                asyncio.ensure_future(aks.producer_client("cancel_order", cancel_order))
            # asyncio.ensure_future(producer.send(req_pipe, key="cancal_order", value=cancel_order))
        return {"code": 20000, "data": aks.cancel_order, "message": "撤单成功"}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        pass


def parse_config(request: Request):
    bootstrap_servers = request.app.state.yaml_config.get("kafka_server", {}).get("bootstrap_servers", [])
    req_pipe = request.app.state.yaml_config.get("kafka_server", {}).get("topics", {}).get("req_pipe")
    return {"bootstrap_servers": bootstrap_servers, "req_pipe": req_pipe}


def login_info(auth_config, user, account, password):
    login = {"type": 3, "reqid": 1, "user": user,
             "accountInfo": {"account": account, "pwd": password, "authid": auth_config.authid,
                             "authcode": auth_config.authcode,
                             "ip": auth_config.ip, "port": auth_config.port, "localip": auth_config.localip,
                             "mac": auth_config.mac,
                             "pcname": auth_config.pcname, "diskid": auth_config.diskid,
                             "cpuid": auth_config.cpuid,
                             "pi": auth_config.pi, "vol": auth_config.vol, "clientname": auth_config.clientname,
                             "clientversion": auth_config.clientversion}}

    return login


@router.post("/account_login", response_model=schemas.Msg, description="资金账号登录", summary="资金账号登录")
async def twap_pov_account_login(request: Request, user: str = Form(...), account: str = Form(...),
                                 password: str = Form(...),
                                 current_user: User = Depends(
                                     deps.AccessPrivilegesPermissions(permission=2, error_msg="资金账号登录"))):
    logger.info("account_login")
    logger.info(current_user.username)
    config = parse_config(request)
    producer = AIOKafkaProducer(bootstrap_servers=config.get("bootstrap_servers"),
                                key_serializer=lambda k: json.dumps(k).encode(),
                                value_serializer=lambda v: json.dumps(v).encode())
    try:
        await producer.start()
        request.app.state.aks.login_resp.clear()
        account_login = login_info(user, account, password)
        asyncio.ensure_future(producer.send(config.get("req_pipe"), key="user-login", value=account_login))
        await asyncio.sleep(1)
        return {"code": 20000, "data": request.app.state.aks.login_resp, "message": "资金账号登录"}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await producer.stop()


@router.get("/account_logout", response_model=schemas.Msg, description="资金账号退出", summary="资金账号退出")
async def twap_pov_account_logout(request: Request, user: str = Form(...), account: str = Form(...),
                                  current_user: User = Depends(
                                      deps.AccessPrivilegesPermissions(permission=2, error_msg="资金账号退出"))):
    logger.info("account_logout")
    logger.info(current_user.username)
    config = parse_config(request)
    producer = AIOKafkaProducer(bootstrap_servers=config.get("bootstrap_servers"),
                                key_serializer=lambda k: json.dumps(k).encode(),
                                value_serializer=lambda v: json.dumps(v).encode())
    try:
        await producer.start()
        account_login = login_info(user, account, "")
        asyncio.ensure_future(producer.send(config.get("req_pipe"), key="user-logout", value=account_login))
        await asyncio.sleep(1)
        return {"code": 20000, "data": request.app.state.aks.login_resp, "message": "资金账号退出"}
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await producer.stop()
