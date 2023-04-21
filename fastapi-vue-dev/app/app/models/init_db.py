import datetime
import os

from sqlalchemy import Integer, String, Column, Sequence, Float, DateTime, ForeignKey, Table, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import Identity
from sqlalchemy.sql import func

from app.db.base_class import Base


# ss_table = Table('T_W_BJHY_STRATEGY_STOCK', Base.metadata,
#                  Column('str_id', Integer, ForeignKey('T_W_BJHY_STRATEGY.sid')),
#                  Column('sto_id', Integer, ForeignKey('T_W_BJHY_STOCK.sid')),
#                  )


class Strategy(Base):
    __tablename__ = "T_W_BJHY_STRATEGY"
    __table_args__ = {'comment': '策略表'}
    # sid = Column(Integer, Sequence(__tablename__ + '_ID_SEQ', start=1, increment=1), primary_key=True, comment="主键")
    sid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    straname = Column(String(256), nullable=False, comment="策略名称", unique=True)
    type = Column(Integer, nullable=True, comment="类型")
    price = Column(Float, nullable=True, comment="价格")
    vol = Column(Integer, nullable=True, comment="数量")
    period = Column(Integer, nullable=True, comment="周期")
    step = Column(Integer, nullable=True, comment="步长")
    cancel = Column(Integer, nullable=True, comment="取消时间")
    limit = Column(Integer, nullable=True, comment="是否限制完成")
    status = Column(Integer, nullable=True, comment="状态")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    # stock = relationship('Stock', secondary="T_W_BJHY_STRATEGY_STOCK", backref=backref("strategys", uselist=False))
    # stock = relationship('Stock', secondary="T_W_BJHY_STRATEGY_STOCK", backref=backref("strategys"))
    # stock = relationship('Stock', secondary='T_W_BJHY_STRATEGY_STOCK', backref=backref("strategys"),
    #                      primaryjoin='Strategy_Stock.str_id==Strategy.sid', lazy='select')
    # stock = relationship('Stock', secondary='T_W_BJHY_STRATEGY_STOCK', backref=backref("strategys"),
    #                      primaryjoin='Strategy_Stock.sto_id==Stock.sid', lazy='dynamic')
    # stockinfo = relationship('Strategy_Stock_Info', secondary='T_W_BJHY_STRATEGY_STOCK', backref=backref("strate"),
    #                          overlaps="stock, T_W_BJHY_STRATEGY",
    #                      primaryjoin='and_(Strategy_Stock.str_id==Strategy.sid)', lazy='select')
    # stockinfo = relationship('Strategy_Stock_Info', backref=backref("strate", uselist=False), secondary="T_W_BJHY_STRATEGY_STOCK")

    def __repr__(self):
        return f"sid:{self.sid},straname:{self.straname},type:{self.type},price:{self.price}," \
               f"period:{self.period},step:{self.step},cancel:{self.cancel},limit:{self.limit},ctime:{self.ctime},status:{self.status},"


class Stock(Base):
    __tablename__ = "T_W_BJHY_STOCK"
    __table_args__ = {'comment': '股票表'}
    # sid = Column(Integer, Sequence(__tablename__ + '_ID_SEQ', start=1, increment=1), primary_key=True, comment="主键")
    sid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    stockcode = Column(String(20), nullable=True, comment="股票代码")
    stockname = Column(String(100), nullable=True, comment="股票名称")
    exchangeid = Column(Integer, nullable=True, comment="市场")
    bsdir = Column(Integer, nullable=True, comment="买卖方向")
    vol = Column(Integer, nullable=True, comment="数量")
    price = Column(Float, nullable=True, comment="价格")
    status = Column(Integer, nullable=True, comment="状态")
    ssi_id = Column(Integer, ForeignKey("T_W_BJHY_STRATEGY_STOCK_INFO.ssiid", ondelete='CASCADE'), comment="外键信息id")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    # sinfo = relationship('Strategy_Stock_Info', secondary="T_W_BJHY_STRATEGY_STOCK", backref=backref("sinfo"))
    # strategy = relationship('Strategy', secondary="T_W_BJHY_STRATEGY_STOCK", backref=backref("strategy", uselist=False))

    def __repr__(self):
        return f"sid:{self.sid},stockcode:{self.stockcode},exchangeid:{self.exchangeid},bsdir:{self.bsdir}," \
               f"vol:{self.vol},ctime:{self.ctime},status:{self.status}"


class Strategy_Stock_Info(Base):
    __tablename__ = "T_W_BJHY_STRATEGY_STOCK_INFO"
    __table_args__ = {'comment': '信息表'}
    # ssiid = Column(Integer, Sequence(__tablename__ + '_ID_SEQ', start=1, increment=1), primary_key=True, comment="主键")
    ssiid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    # ssname = Column(String(256), nullable=True, unique=True, comment="组合名称")
    ssname = Column(String(256), nullable=True, comment="组合名称")
    status = Column(Integer, nullable=False, comment="状态")
    buylock = Column(Boolean, nullable=True, comment="买入锁定", default=False)
    selllock = Column(Boolean, nullable=True, comment="卖出锁定", default=False)
    userid = Column(Integer, ForeignKey("T_W_BJHY_USERS.userid", ondelete='CASCADE'), comment="外键用户id", nullable=True)
    stock = relationship('Stock', backref=backref("ssinfo", uselist=False))
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())
    UniqueConstraint('ssname', 'userid', name='uix_1')

    def __repr__(self):
        return f"ssiid:{self.ssiid},ssname:{self.ssname},ctime:{self.ctime},status:{self.status}"


class Strategy_Stock(Base):
    __tablename__ = "T_W_BJHY_STRATEGY_STOCK"
    __table_args__ = {'comment': '关联表'}
    # ssid = Column(Integer, Sequence(__tablename__ + '_ID_SEQ', start=1, increment=1), primary_key=True, comment="主键")
    ssid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    str_id = Column(Integer, ForeignKey("T_W_BJHY_STRATEGY.sid", ondelete='CASCADE'), comment="外键策略id")
    sto_id = Column(Integer, ForeignKey("T_W_BJHY_STOCK.sid", ondelete='CASCADE'), comment="外键股票id")
    ssi_id = Column(Integer, ForeignKey("T_W_BJHY_STRATEGY_STOCK_INFO.ssiid", ondelete='CASCADE'), comment="外键信息id")
    userid = Column(Integer, ForeignKey("T_W_BJHY_USERS.userid", ondelete='CASCADE'), comment="外键用户id", nullable=True)
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    # strategy = relationship('Strategy', backref=backref("strategy"))
    # stock = relationship('Stock', backref=backref("stock"))
    # ssinfo = relationship('Strategy_Stock_Info', backref=backref("ssinfo"))
    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


class Orderno_Sessionid(Base):
    __tablename__ = "T_W_BJHY_ORDERNO_SESSIONID"
    __table_args__ = {'comment': '实时订单表'}
    # rid = Column(Integer, Sequence(__tablename__ + '_ID_SEQ', start=1, increment=1), primary_key=True, comment="主键")
    rid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    ssid = Column(Integer, ForeignKey("T_W_BJHY_STRATEGY_STOCK.ssid", ondelete='CASCADE'), comment="关联表id", index=True)
    userid = Column(Integer, ForeignKey("T_W_BJHY_USERS.userid", ondelete='CASCADE'), comment="外键用户id", nullable=True)
    order_id = Column(Integer, nullable=True, comment="订单ID")
    job_id = Column(Integer, nullable=True, comment="拆单ID")
    batid = Column(Integer, nullable=True, comment="批次ID")
    orderno = Column(String(256), nullable=True, comment="orderno")
    sessionid = Column(Integer, nullable=True, comment="sessionid")
    status = Column(Integer, nullable=True, comment="状态")
    error_msg = Column(String(256), nullable=True, comment="错误信息")
    account = Column(String(50), nullable=True, comment="账号")
    bsdir = Column(Integer, nullable=True, comment="买卖方向")
    exchangeid = Column(Integer, nullable=True, comment="市场")
    orderprice = Column(Float, nullable=True, comment="下单价格")
    orderstatus = Column(Integer, nullable=True, comment="订单状态")
    ordervol = Column(Float, nullable=True, comment="下单量")
    securityid = Column(String(10), nullable=True, comment="股票代码")
    tradeprice = Column(Float, nullable=True, comment="交易价格")
    tradevol = Column(Float, nullable=True, comment="交易量")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}

    def __repr__(self):
        return f"ssid:{self.ssid},order_id:{self.order_id},orderno:{self.orderno}" \
               f",sessionid:{self.sessionid},status:{self.status},ctime:{self.ctime}"


class StockCodeInfo(Base):
    __tablename__ = "T_W_BJHY_STOCKCODEINFO"
    __table_args__ = {'comment': '市场股票表'}

    scid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    # scid = Column(Integer, Sequence(__tablename__ + '_ID_SEQ', start=1, increment=1), primary_key=True, comment="主键")
    code = Column(String(20), nullable=True, comment="股票代码", index=True)
    stockcode = Column(String(20), nullable=True, comment="股票代码", index=True)
    stockname = Column(String(256), nullable=True, comment="股票名称", index=True)
    firstletter = Column(String(20), nullable=True, comment="首字母")
    exchangeid = Column(Integer, nullable=True, comment="市场")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    def __repr__(self):
        return f"scid:{self.scid},stockcode:{self.stockcode},exchangeid:{self.exchangeid},stockname:{self.stockname}," \
               f"ctime:{self.ctime}"

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


class User(Base):
    __tablename__ = "T_W_BJHY_USERS"
    # __table_args__ = {'mysql_engine': 'InnoDB'}
    # userid = Column(Integer, Sequence(__tablename__ + '_ID_SEQ', start=100, increment=1), primary_key=True)
    userid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    username = Column(String(256), nullable=True, unique=True)
    useremail = Column(String(100), nullable=False, unique=True)
    user_active = Column(Boolean, default=False)
    userhashed_password = Column(String(300), nullable=False)
    userphonenum = Column(String(32), doc="手机号")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())
    role_id = Column(Integer, ForeignKey("T_W_BJHY_ROLE.rid", ondelete='CASCADE'), comment="外键信息id")
    role = relationship("UserRole", backref="user")

    def __repr__(self):
        return f"userid:{self.userid},username:{self.username},useremail:{self.useremail},user_active:{self.user_active}" \
               f",userhashed_password:{self.userhashed_password},userphonenum:{self.userphonenum}"

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


class AccountInfo(Base):
    __tablename__ = "T_W_BJHY_ACCOUNTINFOS"
    __table_args__ = {'comment': '用户账号表'}
    acid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    # userid = Column(Integer, nullable=True, comment="用户ID", unique=True)
    userid_id = Column(Integer, ForeignKey("T_W_BJHY_USERS.userid", ondelete='CASCADE'), comment="外键用户id",
                       nullable=True)
    uname = Column(String(256), nullable=True, comment="用户名", index=True)
    usernum = Column(String(256), nullable=True, comment="用户号")
    account = Column(String(256), nullable=True, comment="账号")
    password = Column(String(256), nullable=True, comment="密码")
    status = Column(Integer, nullable=True, comment="状态")
    host_id = Column(String(200), ForeignKey("T_W_BJHY_HOST.hostname", ondelete='CASCADE'), comment="外键hostname",
                     nullable=True)
    description = Column(String(256), nullable=True, comment="描述")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    # time_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # csdatetime = Column(DateTime, nullable=True, comment="创建时间", server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"acid:{self.acid},userid_id:{self.userid_id},uname:{self.uname},host_id:{self.host_id}," \
               f"account:{self.account},password:{self.password},status:{self.status},ctime:{self.ctime}"

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


class HostInfo(Base):
    __tablename__ = "T_W_BJHY_HOST"
    __table_args__ = {'comment': '配置管理'}
    hid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    hostname = Column(String(200), nullable=True, comment="配置名称", unique=True)
    oracle_host = Column(String(30), nullable=True, comment="Oracle IP")
    oracle_port = Column(Integer, nullable=True, comment="Oracle端口")
    oracle_user = Column(String(30), nullable=True, comment="Oracle用户名")
    oracle_password = Column(String(30), nullable=True, comment="Oracle密码")
    oracle_service_name = Column(String(20), nullable=True, comment="Oracle服务名称")
    redis_host = Column(String(20), nullable=True, comment="redis IP")
    redis_port = Column(Integer, nullable=True, comment="redis端口")
    redis_db = Column(Integer, nullable=True, comment="redis数据库")
    kafka_bootstrap_servers = Column(String(200), nullable=True, comment="kafka_bootstrap_servers")
    kafka_topic_req_pipe = Column(String(20), nullable=True, comment="kafka_topic_req_pipe")
    kafka_topic_rsp_pipe = Column(String(20), nullable=True, comment="kafka_topic_rsp_pipe")
    kafka_topic_command = Column(String(20), nullable=True, comment="kafka_topic_command")
    zmq_host = Column(String(15), nullable=True, comment="zmq_host")
    zmq_port = Column(Integer, nullable=True, comment="zmq_port")
    influxdb_url = Column(String(100), nullable=True, comment="influxdb_url")
    influxdb_token = Column(String(200), nullable=True, comment="influxdb_token")
    influxdb_org = Column(String(15), nullable=True, comment="influxdb_org")
    influxdb_bucket = Column(String(20), nullable=True, comment="influxdb_bucket")
    twap_limit_count = Column(Integer, nullable=True, comment="twap_limit_count")
    pov_limit_count = Column(Integer, nullable=True, comment="pov_limit_count")
    pov_limit_sub_count = Column(Integer, nullable=True, comment="pov_limit_sub_count")
    description = Column(String(20), nullable=True, comment="描述")
    securitytype = Column(Integer, nullable=True, comment="securitytype", default=2)
    authid = Column(String(20), nullable=True, comment="authid")
    authcode = Column(String(200), nullable=True, comment="authcode")
    ip = Column(String(20), nullable=True, comment="ip")
    port = Column(Integer, nullable=True, comment="port", default=32030)
    localip = Column(String(20), nullable=True, comment="localip")
    mac = Column(String(20), nullable=True, comment="mac")
    pcname = Column(String(20), nullable=True, comment="pcname")
    diskid = Column(String(30), nullable=True, comment="diskid")
    cpuid = Column(String(30), nullable=True, comment="cpuid")
    pi = Column(String(20), nullable=True, comment="pi")
    vol = Column(String(20), nullable=True, comment="vol")
    clientname = Column(String(100), nullable=True, comment="clientname")
    clientversion = Column(String(20), nullable=True, comment="clientversion")
    status = Column(Boolean, nullable=True, comment="状态", default=False)
    account = relationship('AccountInfo', backref=backref("hostinfo"))

    def __repr__(self):
        return f"hostname:{self.hostname},kafka_bootstrap_servers:{self.kafka_bootstrap_servers},status:{self.status}"

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


if __name__ == "__main__":
    import cx_Oracle
    from sqlalchemy import create_engine

    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
    engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=True)
    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
