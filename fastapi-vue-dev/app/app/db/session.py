import os

import cx_Oracle
import pymysql

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

pymysql.install_as_MySQLdb()
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
# engine = create_engine("oracle://fcdb:fcdb@" + dns, encoding='utf-8', echo=True)
engine = create_engine("oracle://bjhy:bjhy@" + dns, echo=False, pool_size=0, max_overflow=-1)
# engine = create_engine("mysql://root:1qaz!QAZ@192.168.101.205/udb?charset=utf8")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
