import datetime
import os

import cx_Oracle
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.strategy_stock import Strategy, Stock, Strategy_Stock, Strategy_Stock_Info
from app.models.user import User, UserRole

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db = SessionLocal()
u = db.query(User).filter(User.userid == 1).first()
print(u.role.rid)
u.per = 2
# print(u.per)
p = db.query(UserRole).filter(UserRole.rid == u.role_id).first()
print(p.permission)
plist = []
for n in p.permission:
    plist.append(n.ptype)
    print(n.ptype)
u.per = plist
setattr(u, 'pers', 19)
print(hasattr(u, 'pers'))
print(getattr(u, 'pers'))
print(u.pers)
# straname = Column(String(256), nullable=False)
# type = Column(Integer, nullable=False)
# price = Column(Float, nullable=False)
# period = Column(Integer, nullable=False)
# step = Column(Integer, nullable=False)
# cancel = Column(Integer, nullable=False)
# limit = Column(Integer, nullable=False)
# cdatetime = Column(DateTime, nullable=True)
# status = Column(Integer, nullable=False)


# stockcode = Column(String(20), nullable=False)
# exchangeid = Column(Integer, nullable=False)
# bsdir = Column(Integer, default=False)
# vol = Column(Integer, nullable=False)
# csdatetime = Column(DateTime, nullable=True)
# status = Column(Integer, nullable=False)
# csdatetime = datetime.datetime.now()
# data = {"straname": "113", "type": 1, "price": 1.22, "period": 300, "step": 300, "cancel": 300, "limit": 0, "status": 0,
#         "cdatetime": csdatetime}
# # s = Strategy_Stock_Info(**data)
#
# data2 = {"stockcode": "600010", "exchangeid": 1, "bsdir": 1, "vol": 500, "status": 1, "csdatetime": csdatetime}
#
# data3 = {"ssname": "组合01", "status": 1, "csdatetime": csdatetime}
# # s.stock = [Stock(**data2)]
# # s = Stock(**data2)
# # ss = Strategy_Stock()
# s = Strategy_Stock_Info(**data3)
# db.add(s)
# db.commit()


# str_obj = db.query(Strategy).filter(Strategy.straname == "113").first()
# logger.info(str_obj.sid)
# s = Stock(**data2)
# s.sinfo = [Strategy_Stock_Info(**data3)]
# s.strategy = [Strategy(**data)]
# db.add(s)
# db.commit()
# sto_obj = db.query(Stock).filter(Stock.sid == 8).first()
# # str_obj = db.query(Strategy_Stock_Info).filter(Strategy_Stock_Info.ssiid == 1).first()
# logger.info(sto_obj.sinfo)
# # logger.info(str_obj.ssinfo)
# # print(str_obj.stockinfo)
# db.commit()
# db.close()


# records = (session.query(A) \
#     .outerjoin(B, A.id == B.doc_id)\
#     .outerjoin(C, B.goods_id == C.id)\
#     .filter(C.shop_id == shop_id) \
#     .filter(C.shop_supplier_id == supplier_id) \
#     .filter(A.num.contains(num_key)) \
#     .all())

# from sqlalchemy.orm import aliased
# creator = aliased(User)
#
# modifier = aliased(User)
#
# session.query(creator.name.label(creatorname)(注解:label给属性取别名),modifier.name).join(creator,creator.id==ap.id,isouter=true)(isouter=true表示左连接).join(modifier,modifier.id==ap.id,isouter=true)
