import asyncio
import base64
import os
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from typing import Any, List

import aioredis
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Request, File, UploadFile, BackgroundTasks, Form, Response
from loguru import logger
from sqlalchemy import DATE, or_
from sqlalchemy import cast
from sqlalchemy.orm.session import Session
from starlette.background import BackgroundTask
from starlette.responses import FileResponse, RedirectResponse
from pypinyin import lazy_pinyin, Style

from app import crud
from app import schemas
from app.api.auth_api import deps
from app.api.twap_pov_api.twap_pov_fast_api import send_msg
from app.models.strategy_stock import Strategy, Stock, Strategy_Stock, Strategy_Stock_Info, StockCodeInfo, AccountInfo, \
    Orderno_Sessionid
from app.models.user import User
from app.zmq_client.asyncio_zmq_redis import async_zmq_recv

router = APIRouter()


@router.get("/stra_list", response_model=schemas.Msg, status_code=200, description="查询策略列表", summary="查询策略列表")
async def get_strategy_list(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=4, error_msg="查询策略列表"))
) -> Any:
    logger.info("stra_list")
    logger.info(current_user.username)
    try:
        stra_list = db.query(Strategy.sid, Strategy.straname).filter(Strategy.status == 1).all()
        return {"code": 20000, "data": stra_list, "message": "查询策略列表成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/put_stra", response_model=schemas.Msg, status_code=200, description="新增策略", summary="新增策略")
async def put_strategy(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=4, error_msg="新增策略")),
        stra_in: schemas.order.StraCreate) -> Any:
    logger.info("put_stra")
    logger.info(stra_in)
    logger.info(current_user.username)
    try:
        stra = crud.stra.create(db=db, obj_in=stra_in)
        return {"code": 20000, "data": stra, "message": "新增策略成功"}
    except Exception as e:
        # print(e)
        # raise HTTPException(status_code=400, detail={"code": 40000, "data": None, "message": "策略名称重复"})
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))
        # raise HTTPException(status_code=400, detail="策略名称重复")


@router.post("/add_stra_name", response_model=schemas.Msg, status_code=200, description="新增策略名称", summary="新增策略名称")
async def add_stra_name(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=4, error_msg="新增策略名称")),
        stra_in: schemas.order.StraNameBase) -> Any:
    logger.info("add_stra_name")
    logger.info(stra_in)
    logger.info(current_user.username)
    try:
        if stra_in.straname == "新增策略":
            stra_in.status = None
            obj = db.query(Strategy).filter(Strategy.straname == "新增策略").first()
            if not obj:
                stra = crud.stra.create(db=db, obj_in=stra_in)
                return {"code": 20000, "data": stra, "message": "新增策略成功"}
            else:
                db.delete(obj)
                db.commit()
                stra = crud.stra.create(db=db, obj_in=stra_in)
                return {"code": 20000, "data": stra, "message": "新增策略成功"}
        else:
            stra = crud.stra.create(db=db, obj_in=stra_in)
            return {"code": 20000, "data": stra, "message": "新增策略成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/update_stra_name", response_model=schemas.Msg, status_code=200, description="更新策略名称",
             summary="更新策略名称")
async def update_stra_name(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=4, error_msg="更新策略名称")),
        stra_in: schemas.order.StraNameBase) -> Any:
    logger.info("update_stra_name")
    logger.info(stra_in)
    logger.info(current_user.username)
    try:
        if stra_in.sid is not None:
            stra_data_update = {
                "straname": stra_in.straname,
            }

            db_obj = Strategy(**stra_in.dict())
            stra = crud.stra.update(db=db, db_obj=db_obj, obj_in=stra_data_update)

        return {"code": 20000, "data": stra, "message": "更新组合名称成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/stra_update", response_model=schemas.Msg, status_code=200, description="更新策略", summary="更新策略")
async def update_strategy(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=4, error_msg="更新策略")),
        stra_db: schemas.order.StraUpdate) -> Any:
    logger.info("stra_update")
    logger.info(stra_db.dict())
    logger.info(current_user.username)
    try:
        obj_in = {
            "straname": stra_db.straname,
            "type": stra_db.type,
            "period": stra_db.period,
            "step": stra_db.step,
            "cancel": stra_db.cancel,
            "limit": stra_db.limit,
            "status": stra_db.status
        }
        db_obj = Strategy(**stra_db.dict())
        stra = crud.stra.update(db=db, db_obj=db_obj, obj_in=obj_in)
        return {"code": 20000, "data": stra, "message": "更新策略成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/stra_del", response_model=schemas.Msg, status_code=200, description="更新策略状态", summary="更新策略状态")
async def del_strategy(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=4, error_msg="更新策略状态")),
        stra_db: schemas.order.StraDel) -> Any:
    logger.info("stra_del")
    logger.info(stra_db)
    logger.info(current_user.username)
    try:
        if stra_db.sid is not None:
            obj_in = {
                "status": 0
            }
            db_obj = Strategy(**stra_db.dict())
            crud.stra.update(db=db, db_obj=db_obj, obj_in=obj_in)
            return {"code": 20000, "data": {"status": "success"}, "message": "状态移除成功"}
        return {"code": 20000, "data": {"status": "success"}, "message": "sid不存在"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/stra_remove", response_model=schemas.Msg, status_code=200, description="删除策略", summary="删除策略")
async def remove_strategy(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=4, error_msg="删除策略")),
        stra_in: schemas.order.StraRemove) -> Any:
    logger.info("stra_remove")
    logger.info(stra_in)
    logger.info(current_user.username)
    try:
        crud.stra.remove(db=db, sid=stra_in.sid)
        return {"code": 20000, "data": {"status": "success"}, "message": "删除成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/get_stra", response_model=schemas.Msg, status_code=200, description="查询策略信息", summary="查询策略信息")
async def get_strategy(
        *,
        str_id: int = None,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=4, error_msg="查询策略信息"))
) -> Any:
    logger.info("get_stra")
    logger.info(current_user.username)
    try:
        stra = {}
        if str_id is not None:
            stra = db.query(Strategy).filter(Strategy.sid == str_id, Strategy.status == 1).first()
        return {"code": 20000, "data": stra, "message": "查询策略信息成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/combine_put", response_model=schemas.Msg, status_code=200, description="新建组合名称", summary="新建组合名称")
async def put_combine(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="新建组合名称")),
        combine_in: schemas.combine.CombinenameCreate) -> Any:
    logger.info("combine_put")
    logger.info(combine_in)
    logger.info(current_user.username)
    try:
        # permission_status = 5
        # if hasattr(current_user, 'permission'):
        #     if permission_status not in getattr(current_user, 'permission'):
        #         return {"code": 40000, "data": {"status": "fail"}, "message": "用户无组合权限"}
        userid = current_user.userid
        setattr(combine_in, 'userid', userid)
        combine = crud.combine.create(db=db, obj_in=combine_in)
        return {"code": 20000, "data": combine, "message": "组合名称添加成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/update_combine_name", response_model=schemas.Msg, status_code=200, description="更新组合名称",
             summary="更新组合名称")
async def combine_name_update(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="更新组合名称")),
        combine_in: schemas.combine.CombinenameUpdate) -> Any:
    logger.info("update_combine_name")
    logger.info(combine_in)
    logger.info(current_user.username)
    try:
        if combine_in.ssi_id is not None:
            sk_data_update = {
                "ssname": combine_in.ssname,
            }
            db.query(Strategy_Stock_Info).filter(Strategy_Stock_Info.ssiid == combine_in.ssi_id).update(sk_data_update)
            db.commit()
            db.flush()
        return {"code": 20000, "data": combine_in, "message": "更新组合名称成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/get_order_list", response_model=schemas.Msg, status_code=200, description="查询组合列表", summary="查询组合列表")
async def get_order_list_combine_order_sk(
        *,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="查询组合列表")),
        db: Session = Depends(deps.get_db)) -> Any:
    logger.info("get_order_list")
    logger.info(current_user.username)
    try:
        userid = current_user.userid
        records = db.query(Strategy_Stock_Info.ssname,
                           Strategy.straname, Strategy.type, Strategy.period,
                           Strategy.step, Strategy.cancel, Strategy.limit,
                           Stock.stockcode, Stock.price, Stock.vol, Stock.bsdir, Stock.exchangeid,
                           Strategy_Stock.ssid, Strategy_Stock.str_id, Strategy_Stock.sto_id, Strategy_Stock.ssi_id) \
            .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
            .join(Stock, Stock.sid == Strategy_Stock.sto_id) \
            .join(Strategy, Strategy.sid == Strategy_Stock.str_id) \
            .filter(Strategy_Stock.userid == userid).all()

        return {"code": 20000, "data": records, "message": "查询组合成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/get_order_info", response_model=schemas.Msg, status_code=200, description="查询组合信息", summary="查询组合信息")
async def get_order_info(
        *,
        ssi_id: int = None,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="查询组合信息")),
        db: Session = Depends(deps.get_db)) -> Any:
    try:
        logger.info("get_order_info")
        logger.info(current_user.username)
        if ssi_id is None:
            return {"code": 40000, "data": {"status": "fail"}, "message": "ssi_id不能为空"}
        userid = current_user.userid
        bs_res_obj = db.query(Strategy_Stock_Info.buylock, Strategy_Stock_Info.selllock).filter(
            Strategy_Stock_Info.userid == userid,
            Strategy_Stock_Info.ssiid == ssi_id).first()
        if bs_res_obj is None:
            bs_res = {"buylock": False, "selllock": False}
        else:
            bs_res = {"buylock": bs_res_obj.buylock, "selllock": bs_res_obj.selllock}
        res = db.query(Strategy_Stock_Info).filter(Strategy_Stock_Info.ssiid == ssi_id).first()
        if not res:
            return {"code": 40000, "data": {"status": "fail"}, "message": "ssi_id不能为空不存在"}
        # records = db.query(Strategy_Stock.str_id) \
        #     .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
        #     .filter(Strategy_Stock.userid == userid) \
        #     .filter(Strategy_Stock_Info.ssiid == ssi_id).all()
        stockinfos = db.query(Strategy_Stock_Info.ssname,
                              Stock.stockcode, Stock.stockname, Stock.price, Stock.vol, Stock.bsdir, Stock.exchangeid,
                              Strategy_Stock.ssid, Strategy_Stock.str_id, Strategy_Stock.sto_id, Strategy_Stock.ssi_id) \
            .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
            .join(Stock, Stock.sid == Strategy_Stock.sto_id) \
            .filter(Strategy_Stock.userid == userid) \
            .filter(Strategy_Stock_Info.ssiid == ssi_id).all()
        strategyinfos = db.query(
            Strategy.straname, Strategy.type, Strategy.period,
            Strategy.step, Strategy.cancel, Strategy.limit,
            Strategy_Stock.ssid, Strategy_Stock.str_id, Strategy_Stock.sto_id, Strategy_Stock.ssi_id) \
            .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
            .join(Strategy, Strategy.sid == Strategy_Stock.str_id) \
            .filter(Strategy_Stock.userid == userid) \
            .filter(Strategy_Stock_Info.ssiid == ssi_id).all()
        # records = db.query(Strategy_Stock_Info.ssname,
        #                    Strategy.straname, Strategy.type, Strategy.period,
        #                    Strategy.step, Strategy.cancel, Strategy.limit,
        #                    Stock.stockcode, Stock.stockname, Stock.price, Stock.vol, Stock.bsdir, Stock.exchangeid,
        #                    Strategy_Stock.ssid, Strategy_Stock.str_id, Strategy_Stock.sto_id, Strategy_Stock.ssi_id) \
        #     .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
        #     .join(Stock, Stock.sid == Strategy_Stock.sto_id) \
        #     .join(Strategy, Strategy.sid == Strategy_Stock.str_id) \
        #     .filter(Strategy_Stock.userid == userid) \
        #     .filter(Strategy_Stock_Info.ssiid == ssi_id).all()
        records = []
        for sinfo in stockinfos:
            data = {
                "ssname": sinfo.ssname,
                "straname": None,
                "type": None,
                "step": None,
                "cancel": None,
                "limit": None,
                "stockcode": sinfo.stockcode,
                "stockname": sinfo.stockname,
                "price": sinfo.price,
                "vol": sinfo.vol,
                "bsdir": sinfo.bsdir,
                "exchangeid": sinfo.exchangeid,
                "ssid": sinfo.ssid,
                "str_id": sinfo.str_id,
                "sto_id": sinfo.sto_id,
                "ssi_id": sinfo.ssi_id
            }
            for stra in strategyinfos:
                if sinfo.str_id == stra.str_id:
                    data.update({
                        "str_id": stra.str_id,
                        "straname": stra.straname,
                        "type": stra.type,
                        "step": stra.step,
                        "cancel": stra.cancel,
                        "limit": stra.limit,
                    })
            records.append(data)
        if len(stockinfos) == 0:
            records = [
                # {
                #     "ssname": res.ssname,
                #     "straname": None,
                #     "type": None,
                #     "step": None,
                #     "cancel": None,
                #     "limit": None,
                #     "stockcode": None,
                #     "stockname": None,
                #     "price": None,
                #     "vol": None,
                #     "bsdir": None,
                #     "exchangeid": None,
                #     "ssid": None,
                #     "str_id": None,
                #     "sto_id": None,
                #     "ssi_id": res.ssiid
                # }
            ]
            return {"code": 20000, "data": {"bs_res": bs_res, "records": records}, "message": "查询组合信息成功,无结果返回"}
        return {"code": 20000, "data": {"bs_res": bs_res, "records": records}, "message": "查询组合信息成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/order_put", response_model=schemas.Msg, status_code=200, description="添加组合", summary="添加组合")
async def put_combine_order_sk(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="添加组合")),
        combine_in: schemas.combine.CombineinfoCreate) -> Any:
    logger.info("order_put")
    logger.info(combine_in)
    logger.info(current_user.username)
    try:
        sk_data = {
            "stockcode": combine_in.stockcode,
            "exchangeid": combine_in.exchangeid,
            "price": combine_in.price,
            "vol": combine_in.vol,
            "ctime": combine_in.ctime,
            "status": combine_in.status,
            "ssi_id": combine_in.ssiid,
        }
        sk_obj = Stock(**sk_data)
        db.add(sk_obj)
        db.commit()
        db.flush(sk_obj)
        ss_data = {
            "str_id": combine_in.sid,
            "sto_id": sk_obj.sid,
            "ssi_id": combine_in.ssiid,
        }
        ss_obj = Strategy_Stock(**ss_data)
        db.add(ss_obj)
        db.commit()
        db.flush(ss_obj)
        sk_data["ssid"] = ss_obj.ssid

        return {"code": 20000, "data": sk_data, "message": "组合添加成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/order_line", response_model=schemas.Msg, status_code=200, description="添加行数据", summary="添加行数据")
async def order_line_combine_order_sk(
        *,
        ssi_id: int,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="添加行数据")),
        db: Session = Depends(deps.get_db)) -> Any:
    logger.info("order_line")
    logger.info(current_user.username)
    try:
        userid = current_user.userid
        sk_data = {
            "stockcode": None,
            "stockname": None,
            "exchangeid": None,
            "price": None,
            "vol": None,
            "ctime": None,
            "status": None,
            "ssi_id": int(ssi_id),
        }
        sk_obj = Stock(**sk_data)
        db.add(sk_obj)
        db.commit()
        db.flush(sk_obj)
        ss_data = {
            "str_id": None,
            "sto_id": None,
            "ssi_id": int(ssi_id),
            "userid": userid,
        }
        ss_obj = Strategy_Stock(**ss_data)
        db.add(ss_obj)
        db.commit()
        db.flush(ss_obj)

        return {"code": 20000, "data": {"sto_id": sk_obj.sid, "ssid": ss_obj.ssid}, "message": "添加行数据成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/order_update", response_model=schemas.Msg, status_code=200, description="更新组合", summary="更新组合")
async def update_combine_order_sk(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="更新组合")),
        combine_in: schemas.combine.CombineUpdate) -> Any:
    logger.info("order_update")
    logger.info(combine_in)
    logger.info(current_user.username)
    try:
        if combine_in.sto_id is not None:
            sk_data_update = {
                "stockcode": combine_in.stockcode,
                "stockname": combine_in.stockname,
                "exchangeid": combine_in.exchangeid,
                "price": combine_in.price,
                "vol": combine_in.vol,
                "status": combine_in.status,
                "ssi_id": combine_in.ssi_id,
                "bsdir": combine_in.bsdir
            }
            if len(combine_in.stockcode) == 6:
                if combine_in.stockcode.startswith("6"):
                    sk_data_update["exchangeid"] = 1
                elif combine_in.stockcode.startswith("0") or combine_in.stockcode.startswith("3"):
                    sk_data_update["exchangeid"] = 2
                else:
                    sk_data_update["exchangeid"] = 0

            db.query(Stock).filter(Stock.sid == combine_in.sto_id).update(sk_data_update)
            db.commit()
            db.flush()
            ss_data_update = {
                "str_id": combine_in.str_id,
                "sto_id": combine_in.sto_id,
                "ssi_id": combine_in.ssi_id,
            }
            db.query(Strategy_Stock).filter(Strategy_Stock.ssid == combine_in.ssid).update(ss_data_update)
            db.commit()
            db.flush()
            res = db.query(Strategy.straname, Strategy.type, Strategy.period, Strategy.step, Strategy.cancel,
                           Strategy.limit).filter(Strategy.sid == combine_in.str_id).first()
            if res:
                combine_in.straname = res.straname
                combine_in.type = res.type
                combine_in.period = res.period
                combine_in.step = res.step
                combine_in.cancel = res.cancel
                combine_in.limit = res.limit

        return {"code": 20000, "data": combine_in, "message": "更新组合成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/order_del", response_model=schemas.Msg, status_code=200, description="更新组合状态", summary="更新组合状态")
async def del_combine_order_sk(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="更新组合状态")),
        combine_in: schemas.combine.CombineDel) -> Any:
    logger.info("order_del")
    logger.info(combine_in)
    logger.info(current_user.username)
    try:
        update_status = {
            "status": 0
        }
        sto_id = combine_in.sto_id
        ssi_id = combine_in.ssi_id
        db.query(Stock).filter(Stock.sid == sto_id).update(update_status)
        db.commit()
        db.flush()
        db.query(Strategy_Stock_Info).filter(Strategy_Stock_Info.ssiid == ssi_id).update(update_status)
        db.commit()
        db.flush()

        return {"code": 20000, "data": {"status": "success"}, "message": "更新组合状态成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/order_remove", response_model=schemas.Msg, status_code=200, description="删除组合", summary="删除组合")
async def remove_combine_order_sk(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="删除组合")),
        combine_in: schemas.combine.CombineRemove) -> Any:
    logger.info("order_remove")
    logger.info(combine_in)
    logger.info(current_user.username)
    try:
        ssid = combine_in.ssid
        sto_id = combine_in.sto_id
        ssi_id = combine_in.ssi_id
        obj = db.query(Stock).get(sto_id)
        if not obj:
            return {"code": 40000, "data": {"status": "fail"}, "message": "删除组合错误"}
        db.delete(obj)
        obj = db.query(Strategy_Stock_Info).get(ssi_id)
        if not obj:
            return {"code": 40000, "data": {"status": "fail"}, "message": "删除组合错误"}
        db.delete(obj)
        obj = db.query(Strategy_Stock).get(ssid)
        if not obj:
            return {"code": 40000, "data": {"status": "fail"}, "message": "删除组合错误"}
        db.delete(obj)
        db.commit()

        return {"code": 20000, "data": {"status": "success"}, "message": "删除组合成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/order_remove_all", response_model=schemas.Msg, status_code=200, description="删除组合", summary="删除组合")
async def remove_all_combine_order_sk(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="删除组合")),
        combine_in: schemas.combine.CombineRemove) -> Any:
    logger.info("order_remove_all")
    logger.info(combine_in)
    logger.info(current_user.username)
    try:
        if combine_in.ssi_id:
            records = db.query(Strategy_Stock.ssid, Strategy_Stock_Info.ssiid, Strategy_Stock.sto_id) \
                .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
                .filter(Strategy_Stock_Info.ssiid == combine_in.ssi_id).all()
            obj = db.query(Strategy_Stock_Info).get(combine_in.ssi_id)
            db.delete(obj)
            if records:
                db.query(Stock).filter(Stock.sid.in_([res.sto_id for res in records])).delete()
            db.commit()
        return {"code": 20000, "data": {"status": "success"}, "message": "删除组合成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/remove_line", response_model=schemas.Msg, status_code=200, description="删除行数据", summary="删除行数据")
async def remove_line_combine_order_sk(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="删除行数据")),
        combine_list_in: List[schemas.combine.CombineRemove]) -> Any:
    logger.info("remove_line")
    logger.info(combine_list_in)
    logger.info(current_user.username)
    try:
        if len(combine_list_in) > 0:
            for combine_in in combine_list_in:
                ssid = combine_in.ssid
                sto_id = combine_in.sto_id
                obj = db.query(Strategy_Stock).get(ssid)
                if obj:
                    db.delete(obj)
                obj = db.query(Stock).get(sto_id)
                if obj:
                    db.delete(obj)
                db.commit()

        return {"code": 20000, "data": {"status": "success"}, "message": "删除行数据成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/get_combin_name", response_model=schemas.Msg, status_code=200, description="查询组合名称", summary="查询组合名称")
async def get_combin_name(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=5, error_msg="查询组合名称"))) -> Any:
    logger.info("get_combin_name")
    logger.info(current_user.username)
    try:
        userid = current_user.userid
        records = db.query(Strategy_Stock_Info).filter(Strategy_Stock_Info.userid == userid).all()
        return {"code": 20000, "data": records, "message": "查询组合名称成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/order_on_id", response_model=schemas.Msg, status_code=200, description="组合下的所有单", summary="组合下的所有单")
async def go_order_by_id(request: Request,
                         *,
                         db: Session = Depends(deps.get_db),
                         current_user: User = Depends(
                             deps.AccessPrivilegesPermissions(permission=2, error_msg="组合下的所有单")),
                         orders: schemas.combine.Orders) -> Any:
    logger.info("order_on_id")
    logger.info(current_user.username)
    try:
        userid = current_user.userid
        if not userid:
            return {"code": 40000, "data": orders, "message": "下单失败,userid不存在."}
        if not orders.ssi_id:
            return {"code": 40000, "data": orders, "message": "下单失败,ssi_id."}
        if not orders.account:
            return {"code": 40000, "data": orders, "message": "下单失败,account不存在."}
        records = db.query(Strategy_Stock_Info.ssname,
                           Strategy.straname, Strategy.type, Strategy.period,
                           Strategy.step, Strategy.cancel, Strategy.limit,
                           Stock.stockcode, Stock.price, Stock.vol, Stock.bsdir, Stock.exchangeid,
                           Strategy_Stock.ssid, Strategy_Stock.str_id, Strategy_Stock.sto_id,
                           Strategy_Stock.ssi_id) \
            .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
            .join(Stock, Stock.sid == Strategy_Stock.sto_id) \
            .join(Strategy, Strategy.sid == Strategy_Stock.str_id) \
            .filter(Strategy_Stock_Info.ssiid == orders.ssi_id) \
            .filter(Strategy_Stock.userid == userid).all()
        data_list = []
        if len(records) > 0:
            data_list = [dict(zip(result.keys(), result)) for result in records]
            code_list = [res.get("stockcode") for res in data_list]
            redis_host = request.app.state.redis_info.get("host")
            redis_port = request.app.state.redis_info.get("port")
            zmq_host = request.app.state.zmq_info.get("host")
            zmq_port = request.app.state.zmq_info.get("port")
            asyncio.ensure_future(async_zmq_recv(code_list, redis_host=redis_host, redis_port=redis_port,
                                                 zmq_host=zmq_host, zmq_port=zmq_port))
            await asyncio.sleep(1)
            aks = request.app.state.aks
            for res in data_list:
                kafka_data = {
                    "userid": userid,
                    "account": orders.account,
                    "ssid": res.get("ssid"),
                    "vol": res.get("vol"), "period": res.get("period"), "step": res.get("step"),
                    "cancel": res.get("cancel"), "type": res.get("type"), "stockcode": res.get("stockcode"),
                    "price": res.get("price"), "bsdir": res.get("bsdir"), "exchangeid": res.get("exchangeid"),
                    "limit": res.get("limit")
                }
                asyncio.ensure_future(aks.producer_client(res.get("ssid"), kafka_data, ptype=2))
            db.query(Strategy_Stock_Info).filter(Strategy_Stock_Info.ssiid == orders.ssi_id).update({
                "status": 2
            })
            db.commit()
            db.flush()
        else:
            return {"code": 20000, "data": data_list, "message": "下单失败,没有记录."}
        return {"code": 20000, "data": data_list, "message": "下单成功"}

    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/order", response_model=schemas.Msg, status_code=200, description="下单", summary="下单")
async def go_order(request: Request,
                   *,
                   db: Session = Depends(deps.get_db),
                   current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=2, error_msg="下单")),
                   orders: schemas.combine.OrderList) -> Any:
    logger.info("order")
    logger.info(current_user.username)
    try:
        userid = current_user.userid
        if not userid:
            return {"code": 20000, "data": orders, "message": "下单失败,userid不存在."}
        if not orders.account:
            return {"code": 40000, "data": orders, "message": "下单失败,account不存在."}
        order_list = orders.order_list
        if len(order_list) > 0:
            for order in order_list:
                ssid = order.ssid
                str_id = order.str_id
                sto_id = order.sto_id
                ssi_id = order.ssi_id
                records = db.query(Strategy_Stock_Info.ssname,
                                   Strategy.straname, Strategy.type, Strategy.period,
                                   Strategy.step, Strategy.cancel, Strategy.limit,
                                   Stock.stockcode, Stock.price, Stock.vol, Stock.bsdir, Stock.exchangeid,
                                   Strategy_Stock.ssid, Strategy_Stock.str_id, Strategy_Stock.sto_id,
                                   Strategy_Stock.ssi_id) \
                    .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
                    .join(Stock, Stock.sid == Strategy_Stock.sto_id) \
                    .join(Strategy, Strategy.sid == Strategy_Stock.str_id) \
                    .filter(Strategy_Stock.ssid == ssid) \
                    .filter(Strategy.sid == str_id) \
                    .filter(Stock.sid == sto_id).filter(Strategy_Stock_Info.ssiid == ssi_id) \
                    .filter(Strategy_Stock.userid == userid).all()
                if len(records) > 0:
                    data_list = [dict(zip(result.keys(), result)) for result in records]
                    code_list = [res.get("stockcode") for res in data_list]
                    redis_host = request.app.state.redis_info.get("host")
                    redis_port = request.app.state.redis_info.get("port")
                    zmq_host = request.app.state.zmq_info.get("host")
                    zmq_port = request.app.state.zmq_info.get("port")
                    asyncio.ensure_future(async_zmq_recv(code_list, redis_host=redis_host, redis_port=redis_port,
                                                         zmq_host=zmq_host, zmq_port=zmq_port))
                    # background_tasks.add_task(async_zmq_recv, code_list, redis_host=redis_host, redis_port=redis_port,
                    #                           zmq_host=zmq_host, zmq_port=zmq_port)
                    await asyncio.sleep(5)
                    aks = request.app.state.aks
                    for res in data_list:
                        kafka_data = {
                            "userid": userid,
                            "account": orders.account,
                            "ssid": res.get("ssid"),
                            "vol": res.get("vol"), "period": res.get("period"), "step": res.get("step"),
                            "cancel": res.get("cancel"), "type": res.get("type"), "stockcode": res.get("stockcode"),
                            "price": res.get("price"), "bsdir": res.get("bsdir"), "exchangeid": res.get("exchangeid"),
                            "limit": res.get("limit")
                        }
                        asyncio.ensure_future(aks.producer_client(res.get("ssid"), kafka_data, ptype=2))
                    db.query(Strategy_Stock_Info).filter(Strategy_Stock_Info.ssiid == ssi_id).update({
                        "status": 2
                    })
                    db.commit()
                    db.flush()
                    order.status = 2
                # job = await aks.create_job_web(data[0])
                # # await job.redis.incr("job_id", amount=1)
                # # job_id = await job.redis.mget("job_id")
                # # job.job_id = int(job_id[0])
                # asyncio.ensure_future(job.job_order_async())
                # print(job)
                else:
                    return {"code": 20000, "data": orders, "message": "下单失败,没有记录."}
            return {"code": 20000, "data": orders, "message": "下单成功"}
        return {"code": 20000, "data": orders, "message": "下单失败,没有记录."}

    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/sk_list", response_model=schemas.Msg, status_code=200, description="股票列表", summary="股票列表")
async def sk_list(
        *,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=6, error_msg="股票列表")),
        db: Session = Depends(deps.get_db)) -> Any:
    logger.info("sk_list")
    logger.info(current_user.username)
    try:
        records = db.query(StockCodeInfo.stockcode, StockCodeInfo.code, StockCodeInfo.stockname,
                           StockCodeInfo.stockname, StockCodeInfo.exchangeid).all()

        return {"code": 20000, "data": records, "message": "查询数据成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/sk_like_list", response_model=schemas.Msg, status_code=200, description="模糊查询", summary="模糊查询")
async def sk_list(
        *,
        keyword: str = None,
        ktype: int = 1,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=6, error_msg="模糊查询")),
        db: Session = Depends(deps.get_db)) -> Any:
    logger.info("sk_like_list")
    logger.info(current_user.username)
    try:
        if ktype == 1:
            # records = db.query(StockCodeInfo.stockcode, StockCodeInfo.code, StockCodeInfo.stockname,
            #                    StockCodeInfo.stockname, StockCodeInfo.firstletter, StockCodeInfo.exchangeid).filter(
            #     StockCodeInfo.code.like(f"{keyword}%")).order_by(StockCodeInfo.code.asc()).all()
            records = db.query(StockCodeInfo.stockcode, StockCodeInfo.code, StockCodeInfo.stockname,
                               StockCodeInfo.stockname, StockCodeInfo.firstletter, StockCodeInfo.exchangeid).filter(
                or_(StockCodeInfo.code.like(f"{keyword}%"), StockCodeInfo.stockname.like(f"%{keyword}%"),
                    StockCodeInfo.firstletter.like(f"{keyword}%"))).order_by(StockCodeInfo.code.asc()).all()

            return {"code": 20000, "data": records, "message": "模糊查询成功"}
        if ktype == 2:
            records = db.query(StockCodeInfo.stockcode, StockCodeInfo.code, StockCodeInfo.stockname,
                               StockCodeInfo.stockname, StockCodeInfo.exchangeid).filter(
                StockCodeInfo.stockname.like(f"%{keyword}%")).order_by(StockCodeInfo.code.asc()).all()

            return {"code": 20000, "data": records, "message": "模糊查询成功"}
        return {"code": 20000, "data": {}, "message": "模糊查询失败"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/user_list", response_model=schemas.Msg, status_code=200, description="用户列表", summary="用户列表")
async def user_list(
        *,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=7, error_msg="用户列表")),
        db: Session = Depends(deps.get_db)) -> Any:
    logger.info("user_list")
    logger.info(current_user.username)
    try:
        records = db.query(User.userid, User.username, User.useremail,
                           User.user_active).all()
        return {"code": 20000, "data": records, "message": "查询数据成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/admin_list", response_model=schemas.Msg, status_code=200, description="交易账号列表", summary="交易账号列表")
async def admin_list(
        *,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=8, error_msg="交易账号列表")),
        db: Session = Depends(deps.get_db)) -> Any:
    logger.info("admin_list")
    logger.info(current_user.username)
    try:
        # records = db.query(AccountInfo.acid, AccountInfo.user_id, AccountInfo.uname, AccountInfo.usernum,
        #                    AccountInfo.account, AccountInfo.password, AccountInfo.status, AccountInfo.description,
        #                    AccountInfo.ctime).all()
        records = db.query(AccountInfo).all()
        return {"code": 20000, "data": records, "message": "查询数据成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/user_admin_list", response_model=schemas.Msg, status_code=200, description="用户资金账号列表", summary="用户资金账号列表")
async def user_admin_list(
        *,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=8, error_msg="用户资金账号列表")),
        db: Session = Depends(deps.get_db)) -> Any:
    logger.info("user_admin_list")
    logger.info(current_user.username)
    try:
        uname = current_user.username
        userid = current_user.userid
        records = db.query(AccountInfo.acid, AccountInfo.userid_id, AccountInfo.uname, AccountInfo.usernum,
                           AccountInfo.account, AccountInfo.password, AccountInfo.status, AccountInfo.description,
                           AccountInfo.ctime).filter(AccountInfo.userid_id == userid,
                                                     AccountInfo.uname == uname).all()
        return {"code": 20000, "data": records, "message": "查询数据成功"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/admin_add", response_model=schemas.Msg, status_code=200, description="新增权限管理", summary="新增权限管理")
async def bjhy_admin(
        *,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=8, error_msg="新增权限管理")),
        user_list: List[schemas.combine.UserList],
        db: Session = Depends(deps.get_db)) -> Any:
    logger.info("admin_add")
    logger.info(current_user.username)
    try:
        if len(user_list) > 0:
            for user in user_list:
                accinfo_data = {
                    "userid": user.userid,
                    "uname": user.uname,
                    "usernum": user.usernum,
                    "account": user.account,
                    "password": user.password,
                    "status": user.status,
                    "accdesc": user.accdesc
                }
                acc_obj = AccountInfo(**accinfo_data)
                db.add(acc_obj)
                db.commit()
                db.flush(acc_obj)

            return {"code": 20000, "data": user_list, "message": "添加账号成功"}
        return {"code": 20000, "data": {}, "message": "添加账号失败"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/admin_update", response_model=schemas.Msg, status_code=200, description="更新权限管理", summary="更新权限管理")
async def admin_update(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=8, error_msg="更新权限管理")),
        admin_in: schemas.combine.AdminUpdate) -> Any:
    logger.info("admin_update")
    logger.info(admin_in)
    logger.info(current_user.username)
    try:
        if admin_in.acid is not None:
            admin_data_update = {
                "userid": admin_in.userid,
                "uname": admin_in.uname,
                "usernum": admin_in.usernum,
                "account": admin_in.account,
                "password": admin_in.password,
                "status": admin_in.status,
                "accdesc": admin_in.accdesc
            }
            db.query(AccountInfo).filter(AccountInfo.acid == admin_in.acid).update(admin_data_update)
            db.commit()
            db.flush()
            return {"code": 20000, "data": admin_in, "message": "更新组合成功"}
        return {"code": 20000, "data": {"status": "fail"}, "message": "失败,acid为空"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/admin_del", response_model=schemas.Msg, status_code=200, description="更新权限管理状态", summary="更新权限管理状态")
async def del_admin(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=8, error_msg="更新权限管理状态")),
        admin_in: schemas.combine.AdminDel) -> Any:
    logger.info("admin_del")
    logger.info(admin_in)
    logger.info(current_user.username)
    try:
        if admin_in.acid is not None:
            update_status = {
                "status": 0
            }

            db.query(AccountInfo).filter(AccountInfo.acid == admin_in.acid).update(update_status)
            db.commit()
            db.flush()
            return {"code": 20000, "data": {"status": "success"}, "message": "更新组合状态成功"}
        return {"code": 20000, "data": {"status": "fail"}, "message": "失败,acid为空"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/admin_remove", response_model=schemas.Msg, status_code=200, description="删除权限管理", summary="删除权限管理")
async def remove_admin(
        *,
        db: Session = Depends(deps.get_db),
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=8, error_msg="删除权限管理")),
        admin_in: schemas.combine.AdminDel) -> Any:
    logger.info("admin_remove")
    logger.info(admin_in)
    logger.info(current_user.username)
    try:
        if admin_in.acid is not None:
            obj = db.query(AccountInfo).get(admin_in.acid)
            db.delete(obj)
            db.commit()
            return {"code": 20000, "data": {"status": "success"}, "message": "删除成功"}
        return {"code": 50000, "data": {"status": "fail"}, "message": "失败,acid为空"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/user_orders", response_model=schemas.Msg, status_code=200, description="用户订单查询", summary="用户订单查询")
async def user_orders(request: Request,
                      *,
                      db: Session = Depends(deps.get_db),
                      current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=3, error_msg="用户订单查询")),
                      user_order: schemas.combine.UserOrders) -> Any:
    logger.info("user_orders")
    logger.info(user_order)
    logger.info(current_user.username)
    try:
        res_data = []
        if current_user.userid:
            if user_order.qtype in [13, 15]:
                request.app.state.aks.query_order.clear()
                aks = request.app.state.aks
                query_order = send_msg(type=user_order.qtype, securityid=user_order.securityid,
                                       account=user_order.account)
                asyncio.ensure_future(aks.producer_client(current_user.userid, query_order, ptype=1))
                await asyncio.sleep(1)
                time_now = datetime.now()
                start_str = time_now.strftime("%Y-%m-%d") + " 00:00:00"
                end_str = time_now.strftime("%Y-%m-%d") + " 23:59:59"
                start = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
                end = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
                record = db.query(Orderno_Sessionid.orderno, Orderno_Sessionid.sessionid). \
                    filter(Orderno_Sessionid.ctime.between(start, end)).filter(
                    Orderno_Sessionid.userid == current_user.userid).all()
                # record = db.query(Orderno_Sessionid.orderno, Orderno_Sessionid.sessionid).filter(
                #     Orderno_Sessionid.ctime >= time_now - timedelta(days=1)).filter(
                #     Orderno_Sessionid.userid == user_order.userid).all()
                # res = db.query(Orderno_Sessionid.orderno, Orderno_Sessionid.sessionid).filter(
                #     cast(Orderno_Sessionid.ctime, DATE) == cast(datetime.now().date(), DATE)).all()
                # record = db.query(Orderno_Sessionid.orderno, Orderno_Sessionid.sessionid).filter(
                #     Orderno_Sessionid.userid == user_order.userid).all()
                orderInfo = request.app.state.aks.query_order.get("orderInfo", [])
                for order in orderInfo:
                    if len(record) > 0:
                        for odi in record:
                            if order.get("orderno") == odi.orderno and order.get("sessionid") == odi.sessionid:
                                res_data.append(order)
                                session_data_update = {
                                    "account": order.get("account"),
                                    "bsdir": order.get("bsdir"),
                                    "exchangeid": order.get("exchangeid"),
                                    "orderprice": order.get("orderprice"),
                                    "orderstatus": order.get("orderstatus"),
                                    "ordervol": order.get("ordervol"),
                                    "securityid": order.get("securityid"),
                                    "tradeprice": order.get("tradeprice"),
                                    "tradevol": order.get("tradevol"),
                                }
                                db.query(Orderno_Sessionid).filter(Orderno_Sessionid.orderno == order.get("orderno"),
                                                                   Orderno_Sessionid.sessionid == order.get(
                                                                       "sessionid")).update(
                                    session_data_update)
                                db.commit()
                                db.flush()
                return {"code": 20000, "data": res_data, "message": "用户订单查询成功"}
        return {"code": 50000, "data": {"status": "fail"}, "message": "失败,userid为空"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/files")
async def create_files(files: List[bytes] = File()):
    return {"file_sizes": [len(file) for file in files]}


def background_save_file(filename, file_byes):
    localpath = os.path.join(os.path.abspath(".."), "static/file/uploadfiles")
    with open(os.path.join(localpath, filename), "wb") as upfile:
        upfile.write(file_byes)
        upfile.flush()


@router.post("/uploadfiles", description="导入数据", summary="导入数据")
async def create_upload_files(files: List[UploadFile], background_tasks: BackgroundTasks,
                              db: Session = Depends(deps.get_db), str_id: int = Form(), ssi_id: int = Form(),
                              current_user: User = Depends(
                                  deps.AccessPrivilegesPermissions(permission=10, error_msg="导入数据"))) -> Any:
    try:
        logger.info("uploadfiles")
        logger.info(current_user.username)
        if not current_user.userid:
            return {"code": 20000, "data": [], "message": "导入文件失败,userid不存在."}
        if str_id is not None and ssi_id is not None:
            ssid_list = []
            res_data = []
            for file in files:
                contents = await file.read()
                background_tasks.add_task(background_save_file, file.filename, contents)
                if file.content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                    data = pd.read_excel(contents, sheet_name=None)
                    for k, v in data.items():
                        for val in v.to_dict(orient="records"):
                            sk_data = {
                                "stockcode": val.get("股票代码"),
                                "stockname": val.get("股票名称"),
                                "price": val.get("价格"),
                                "vol": val.get("买卖数量"),
                                "bsdir": 0,
                                "status": 1,
                                "ssi_id": ssi_id,
                                "ctime": datetime.now()
                            }
                            if val.get("买卖方向") == "买":
                                sk_data["bsdir"] = 1
                            elif val.get("买卖方向") == "卖":
                                sk_data["bsdir"] = 2
                            stockcode = str(sk_data.get("stockcode", ""))
                            if len(stockcode) == 6:
                                if stockcode.startswith("6"):
                                    sk_data["exchangeid"] = 1
                                elif stockcode.startswith("0") or stockcode.startswith("3"):
                                    sk_data["exchangeid"] = 2
                                else:
                                    sk_data["exchangeid"] = 0
                            sk_obj = Stock(**sk_data)
                            db.add(sk_obj)
                            db.commit()
                            db.flush(sk_obj)
                            ss_data = {
                                "str_id": str_id,
                                "sto_id": sk_obj.sid,
                                "ssi_id": ssi_id,
                                "userid": current_user.userid
                            }
                            ss_obj = Strategy_Stock(**ss_data)
                            db.add(ss_obj)
                            db.commit()
                            db.flush(ss_obj)
                            ssid_list.append(ss_obj.ssid)
                records = db.query(
                    Strategy_Stock.ssid, Strategy_Stock.str_id, Strategy_Stock.sto_id,
                    Strategy_Stock.ssi_id, Strategy_Stock_Info.ssname, Strategy.straname,
                    Stock.stockcode, Stock.bsdir, Stock.vol, Stock.price, StockCodeInfo.stockname,
                    Strategy_Stock_Info.status) \
                    .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
                    .join(Stock, Stock.sid == Strategy_Stock.sto_id) \
                    .join(Strategy, Strategy.sid == Strategy_Stock.str_id) \
                    .join(StockCodeInfo, StockCodeInfo.code == Stock.stockcode) \
                    .filter(Strategy_Stock.ssid.in_(ssid_list)) \
                    .all()
                if len(records) > 0:
                    res_data.extend(records)
                    return {"code": 20000, "data": res_data, "message": "导入文件成功"}
                elif file.content_type == "text/csv":
                    # df = pd.read_csv(StringIO(contents.decode("utf-8")))
                    df = pd.read_csv(BytesIO(contents))
                    for val in df.to_dict(orient="records"):
                        sk_data = {
                            "stockcode": val.get("股票代码"),
                            "stockname": val.get("股票名称"),
                            "price": val.get("价格"),
                            "vol": val.get("买卖数量"),
                            "bsdir": 0,
                            "status": 1,
                            "ssi_id": ssi_id,
                            "ctime": datetime.now()
                        }
                        if val.get("买卖方向") == "买":
                            sk_data["bsdir"] = 1
                        elif val.get("买卖方向") == "卖":
                            sk_data["bsdir"] = 2
                        stockcode = str(sk_data.get("stockcode", ""))
                        if len(stockcode) == 6:
                            if stockcode.startswith("6"):
                                sk_data["exchangeid"] = 1
                            elif stockcode.startswith("0") or stockcode.startswith("3"):
                                sk_data["exchangeid"] = 2
                            else:
                                sk_data["exchangeid"] = 0
                        sk_obj = Stock(**sk_data)
                        db.add(sk_obj)
                        db.commit()
                        db.flush(sk_obj)
                        ss_data = {
                            "str_id": str_id,
                            "sto_id": sk_obj.sid,
                            "ssi_id": ssi_id,
                        }
                        ss_obj = Strategy_Stock(**ss_data)
                        db.add(ss_obj)
                        db.commit()
                        db.flush(ss_obj)
                    records = db.query(
                        Strategy_Stock.ssid, Strategy_Stock.str_id, Strategy_Stock.sto_id,
                        Strategy_Stock.ssi_id, Strategy_Stock_Info.ssname, Strategy.straname,
                        Stock.stockcode, Stock.bsdir, Stock.vol, Stock.price, StockCodeInfo.stockname,
                        Strategy_Stock_Info.status) \
                        .join(Strategy_Stock_Info, Strategy_Stock.ssi_id == Strategy_Stock_Info.ssiid) \
                        .join(Stock, Stock.sid == Strategy_Stock.sto_id) \
                        .join(Strategy, Strategy.sid == Strategy_Stock.str_id) \
                        .join(StockCodeInfo, StockCodeInfo.code == Stock.stockcode) \
                        .filter(Strategy_Stock.ssid.in_(ssid_list)) \
                        .all()
                    if len(records) > 0:
                        res_data.extend(records)
                        return {"code": 20000, "data": res_data, "message": "导入文件成功"}
                else:
                    return {"code": 40000, "data": [file.filename for file in files], "message": "文件类型错误"}
        else:
            return {"code": 40000, "data": [file.filename for file in files], "message": "str_id或ssi_id错误"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))

    return {"filenames": [file.filename for file in files]}


@router.post("/download", summary="导出数据", description="导出数据")
async def download_file(*, response: Response,
                        db: Session = Depends(deps.get_db),
                        current_user: User = Depends(
                            deps.AccessPrivilegesPermissions(permission=9, error_msg="导出数据")),
                        orders: schemas.combine.OrderList):
    try:
        logger.info("download")
        logger.info(current_user.username)

        userid = current_user.userid
        if not userid:
            return {"code": 40000, "data": orders, "message": "导出数据失败,userid不存在."}
        order_list = orders.order_list
        if len(order_list) > 0:
            download_data_list = []
            for order in order_list:
                ssid = order.ssid
                sto_id = order.sto_id
                records = db.query(Stock.stockcode, Stock.price, Stock.vol, Stock.bsdir, Stock.exchangeid,
                                   StockCodeInfo.stockname) \
                    .join(Strategy_Stock, Stock.sid == Strategy_Stock.sto_id) \
                    .join(StockCodeInfo, StockCodeInfo.code == Stock.stockcode) \
                    .filter(Strategy_Stock.ssid == ssid) \
                    .filter(Strategy_Stock.userid == userid) \
                    .filter(Stock.sid == sto_id).all()
                if len(records) > 0:
                    data_list = [dict(zip(result.keys(), result)) for result in records]
                    for index, res in enumerate(data_list):
                        download_data = {
                            "序号": index + 1,
                            "股票代码": res.get("stockcode"),
                            "股票名称": res.get("stockname"),
                            "买卖数量": res.get("vol"),
                            "价格": res.get("price"),
                            "买卖方向": "买" if res.get("bsdir") == 1 else "卖"
                        }
                        if res.get("bsdir") == 1:
                            download_data["买卖方向"] = "买"
                        elif res.get("bsdir") == 2:
                            download_data["买卖方向"] = "卖"
                        else:
                            download_data["买卖方向"] = "未知"
                        download_data_list.append(
                            download_data
                        )
                else:
                    return {"code": 40000, "data": orders, "message": "导出数据失败,没有记录."}

            file = os.path.join(os.path.abspath(".."), "static/file/downloads")
            file = os.path.join(file, str(datetime.now().date()) + ".xlsx")
            df = pd.DataFrame(download_data_list)
            df.columns = ["序号", "股票代码", "股票名称", "买卖数量", "价格", "买卖方向"]
            df.to_excel(file, index=False)
            response.headers["Access-Control-Expose-Headers"] = "Content-Disposition"
            headers = {"Access-Control-Expose-Headers": "Content-Disposition"}
            return FileResponse(
                file,
                headers=headers,
                filename=f"股票信息导出-{datetime.now().date()}.xlsx",
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                background=BackgroundTask(lambda: os.remove(file)),
            )

        return {"code": 40000, "data": orders, "message": "导出数据失败,没有记录."}

    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/download_test", summary="下载文件", description="下载文件")
async def download_file_test():
    # print(os.path.dirname(__file__))
    # print(os.getcwd())
    # print(os.path.dirname(os.path.realpath(__file__)))
    # print(os.path.dirname(os.path.abspath(__file__)))
    # print(os.path.abspath(__file__))
    # print(os.path.abspath("."))
    # print(os.path.abspath(".."))
    # print(os.path.abspath(os.curdir))
    buffer = BytesIO()
    result = [
        {
            "id": 1,
            "name": "xxx",
            "age": 18
        }, {
            "id": 2,
            "name": "bbb",
            "age": 19
        }
    ]
    file = os.path.join(os.path.abspath(".."), "static/file/downloads")
    file = os.path.join(file, str(datetime.now().date()) + ".xlsx")
    df = pd.DataFrame(result)
    df.columns = ["序号", "姓名", "年龄"]
    df.to_excel(file, index=False)
    # return FileResponse(file, filename="user.xlsx")
    return FileResponse(
        file,
        filename="user.xlsx",
        background=BackgroundTask(lambda: os.remove(file)),
    )


@router.get("/sk_update", response_model=schemas.Msg, status_code=200, description="股票列表更新", summary="股票列表")
async def sk_update(
        *,
        current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=6, error_msg="股票列表更新")),
        db: Session = Depends(deps.get_db)) -> Any:
    try:
        logger.info("sk_update")
        logger.info(current_user.username)
        sql_origin = """
        SELECT STOCKCODE,SUBSTR(STOCKCODE, INSTR(STOCKCODE, '.', 1, 1) + 1) AS CODE,STOCKNAME,
        CASE  SUBSTR(SUBSTR(STOCKCODE, INSTR(STOCKCODE, '.', 1, 1) + 1),1,1)
        WHEN '6' THEN 1
        WHEN '0' THEN 2
        WHEN '3' THEN 2
        ELSE 0 END AS EXCHANGEID,SYSDATE AS CSDATETIME FROM T_S_THS_STOCK_INDUSTRY@lv1 WHERE ENTRYDATETIME =( SELECT MAX(ENTRYDATETIME) FROM T_S_THS_STOCK_INDUSTRY@lv1) 
        ORDER BY PCHG DESC
        """
        res_origin = db.execute(sql_origin)
        res_database_data = res_origin.fetchall()
        if len(res_database_data) > 0:
            records = db.query(StockCodeInfo.code).all()
            records = [res.code for res in records]
            write_data_list = []
            for rep in res_database_data:
                if rep[1] not in records:
                    write_data_list.append(
                        StockCodeInfo(code=rep[1], stockcode=rep[0], stockname=rep[2],
                                      firstletter="".join(lazy_pinyin(rep[2], style=Style.FIRST_LETTER)),
                                      exchangeid=rep[3],
                                      ctime=rep[4]))
            db.bulk_save_objects(write_data_list)
            db.commit()
            # db.add_all(write_data_list)
            # db.commit()
            return {"code": 20000, "data": write_data_list, "message": "更新成功"}

        return {"code": 20000, "data": [], "message": "无数据更新"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/ts")
async def ts(*, db: Session = Depends(deps.get_db)):
    return RedirectResponse("/bjhy/sk_list", status_code=302)


@router.post("/lock", response_model=schemas.Msg, description="表格锁定", summary="表格锁定")
async def table_lock(lock_table: schemas.combine.CombineTableLock,
                     current_user: User = Depends(deps.AccessPrivilegesPermissions(permission=11, error_msg="表格锁定")),
                     db: Session = Depends(deps.get_db)):
    try:
        logger.info("lock")
        logger.info(current_user.username)
        if lock_table.ssi_id is not None:
            sk_data_update = {
                "buylock": lock_table.buylock,
                "selllock": lock_table.selllock,
            }
            db.query(Strategy_Stock_Info).filter(Strategy_Stock_Info.ssiid == lock_table.ssi_id,
                                                 Strategy_Stock_Info.userid == current_user.userid).update(
                sk_data_update)
            db.commit()
            db.flush()
            return {"code": 20000, "data": sk_data_update, "message": "更新表格锁定成功"}
        return {"code": 40000, "data": {"status": "fail"}, "message": "更新表格锁定失败,ssi_id为空."}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/user_table_lock", response_model=schemas.Msg, description="表格锁定", summary="表格锁定")
async def user_table_lock(request: Request, ltype: int = None,
                          current_user: User = Depends(
                              deps.AccessPrivilegesPermissions(permission=11, error_msg="表格锁定"))):
    try:
        logger.info("lock")
        logger.info(current_user.username)
        if ltype is not None:
            user = current_user
            if ltype == 1:
                redis_login = request.app.state.redis_login
                await redis_login.hset(user.userid, key="lock", value=1)
                return {"code": 20000, "data": {"status": "success"}, "message": "表格锁定成功"}
            elif ltype == 0:
                redis_login = request.app.state.redis_login
                await redis_login.hset(user.userid, key="lock", value=0)
                return {"code": 20000, "data": {"status": "success"}, "message": "取消表格锁定成功"}
        return {"code": 40000, "data": {"status": "fail"}, "message": "ltype不能为空"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/get_sk_price", response_model=schemas.Msg, description="查询股票价格", summary="查询股票价格")
async def get_sk_price(request: Request, stockcode: str = None,
                       current_user: User = Depends(
                           deps.AccessPrivilegesPermissions(permission=11, error_msg="查询股票价格"))):
    try:
        if stockcode is None:
            return {"code": 40000, "data": {"status": "fail"}, "message": "stockcode不能为空"}
        logger.info("get_sk_price")
        logger.info(current_user.username)
        redis_host = request.app.state.redis_info.get("host")
        redis_port = request.app.state.redis_info.get("port")
        redis = aioredis.from_url(f"redis://{redis_host}:{redis_port}", encoding="utf-8", db=0, decode_responses=True)
        security = base64.b64encode(stockcode.encode()).decode()
        lastprice = await redis.hgetall(security)
        return {"code": 20000, "data": lastprice, "message": "查询股票价格"}

    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/get_sk_name", response_model=schemas.Msg, description="查询股票名称", summary="查询股票名称")
async def get_sk_name(request: Request, stockcode: str = None,
                      current_user: User = Depends(
                          deps.AccessPrivilegesPermissions(permission=11, error_msg="查询股票名称")),
                      db: Session = Depends(deps.get_db)):
    try:
        if stockcode is None:
            return {"code": 40000, "data": {"status": "fail"}, "message": "stockcode不能为空"}
        logger.info("get_sk_name")
        logger.info(current_user.username)
        redis_host = request.app.state.redis_info.get("host")
        redis_port = request.app.state.redis_info.get("port")
        redis = aioredis.from_url(f"redis://{redis_host}:{redis_port}", encoding="utf-8", db=7, decode_responses=True)
        stockcode_exists = await redis.exists(stockcode)
        if not stockcode_exists:
            sk_infos = db.query(StockCodeInfo.code, StockCodeInfo.stockname).all()
            if len(sk_infos) > 0:
                for sk in sk_infos:
                    await redis.set(sk.code, sk.stockname)
        stockname = await redis.get(stockcode)
        return {"code": 20000, "data": {stockcode: stockname}, "message": "查询股票名称"}

    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/get_sk_name_all", response_model=schemas.Msg, description="查询所有股票名称", summary="查询所有股票名称")
async def get_sk_name_all(request: Request,
                          current_user: User = Depends(
                              deps.AccessPrivilegesPermissions(permission=11, error_msg="查询所有股票名称")),
                          db: Session = Depends(deps.get_db)):
    try:
        logger.info("get_sk_name_all")
        logger.info(current_user.username)
        redis_host = request.app.state.redis_info.get("host")
        redis_port = request.app.state.redis_info.get("port")
        redis = aioredis.from_url(f"redis://{redis_host}:{redis_port}", encoding="utf-8", db=7, decode_responses=True)
        stockcode_exists = await redis.exists("sk_set_data")
        if not stockcode_exists:
            sk_infos = db.query(StockCodeInfo.code, StockCodeInfo.stockname).all()
            if len(sk_infos) > 0:
                mapping = {}
                for sk in sk_infos:
                    mapping[sk.code] = sk.stockname
                await redis.hset("sk_set_data", mapping=mapping)
                return {"code": 20000, "data": mapping, "message": "查询所有股票名称"}
        else:
            sk_data = await redis.hgetall("sk_set_data")
            return {"code": 20000, "data": sk_data, "message": "查询所有股票名称"}

    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail=str(e))
