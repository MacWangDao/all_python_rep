import os

from sqlalchemy import Integer, String, Column, Sequence, Float, DateTime

from app.db.base_class import Base


class User(Base):
    __tablename__ = "T_W_BJHY_STRATEGY"
    sid = Column(Integer, Sequence(__tablename__ + '_ID_SEQ', start=1, increment=1), primary_key=True)
    stockcode = Column(String(20), nullable=False)
    type = Column(Integer, nullable=False)
    exchangeid = Column(Integer, nullable=False)
    bsdir = Column(Integer, default=False)
    vol = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    period = Column(Integer, nullable=False)
    step = Column(Integer, nullable=False)
    cancel = Column(Integer, nullable=False)
    limit = Column(Integer, nullable=False)
    cdatetime = Column(DateTime, nullable=True)
    status = Column(Integer, nullable=False)


if __name__ == "__main__":
    import cx_Oracle
    from sqlalchemy import create_engine

    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
    engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=True)
    Base.metadata.create_all(engine)
