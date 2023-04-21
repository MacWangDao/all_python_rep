import json

import asyncio
from sqlalchemy.orm.session import Session
from app.models.strategy_stock import Orderno_Sessionid
from app.db.session import SessionLocal
import async_timeout

import aioredis
from loguru import logger

redis_9 = aioredis.from_url("redis://192.168.101.205", db=9, encoding="utf-8", decode_responses=True)
redis_10 = aioredis.from_url("redis://192.168.101.205", db=10, encoding="utf-8", decode_responses=True)
pubsub = redis_10.pubsub()


async def pubsub_redis(db: Session = SessionLocal()):
    logger.info("init")
    # await pubsub.psubscribe("channel:*")
    await pubsub.subscribe("redis_oracle:1")
    while True:
        try:
            message = await pubsub.get_message(ignore_subscribe_messages=True)
            if message is not None:
                order = json.loads(message.get("data"))
                tradevol = order.get("tradevol")
                orderstatus = order.get("orderstatus")
                orderno = order.get("orderno")
                logger.info(order)
                tradevol_status = await redis_9.hmget(orderno, "tradevol", "orderstatus", "to_db", "not_cancel")
                redis_tradevol, redis_status, to_db, not_cancel = tradevol_status
                if to_db is not None and int(to_db) == 0:
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
                    await redis_9.hset(orderno, "to_db", "1")
                elif to_db is not None and int(to_db) == 1:
                    if int(redis_tradevol) != tradevol or int(redis_status) != orderstatus or (
                            not_cancel is not None and int(not_cancel) == 1):
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
        except Exception as e:
            logger.exception(e)


def pubsub_redis_start():
    asyncio.run(pubsub_redis())


if __name__ == "__main__":
    pubsub_redis_start()
