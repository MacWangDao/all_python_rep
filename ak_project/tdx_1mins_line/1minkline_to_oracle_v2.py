import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import pandas as pd
from loguru import logger
from mootdx.reader import Reader
from sqlalchemy import create_engine, text
import configparser
import cx_Oracle
import os
from sqlalchemy import Integer, String, Column, Float, DateTime, Numeric
from sqlalchemy.schema import Identity
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr
import typing as t
import warnings

warnings.filterwarnings("ignore")
logger.add("1minkline_to_oracle.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
class_registry: t.Dict = {}


@as_declarative(class_registry=class_registry)
class Base:
    id: t.Any
    __name__: str

    # Generate __tablename__ automatically
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


class OneMinkLineDB(Base):
    __tablename__ = "T_1MINKLINE_BJHY"
    __table_args__ = {'comment': '一分钟线'}
    cid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    TRADEDT = Column(DateTime, nullable=True, comment="时间")
    STOCKCODE = Column(String(10), nullable=True, comment="股票代码", index=True)
    OPEN = Column(Numeric(15, 6), nullable=True, comment="开盘价")
    HIGH = Column(Numeric(15, 6), nullable=True, comment="最高价")
    LOW = Column(Numeric(15, 6), nullable=True, comment="最低价")
    CLOSE = Column(Numeric(15, 6), nullable=True, comment="收盘价")
    AMOUNT = Column(Float, nullable=True, comment="金额")
    VOLUME = Column(Integer, nullable=True, comment="数量")

    # ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    # uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())

    def __repr__(self):
        return f"STOCKCODE:{self.STOCKCODE}"

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}


def database_init():
    import cx_Oracle
    from sqlalchemy import create_engine
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
    engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=True)
    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


class MinkLine(object):
    def __init__(self, config_path: str = "config.ini"):
        try:
            self.df_list = []
            logger.info("config.ini")
            config = configparser.ConfigParser()
            config.read(config_path, encoding='utf-8')
            now_date_time = config.get('date', 'now_date_time')
            if len(now_date_time) == 0:
                now_date_time = datetime.datetime.today().strftime("%Y%m%d")
            # now_date_time = "20230227"
            logger.info(now_date_time)
            self.date_start = self.tdfdt_to_pddt(now_date_time, '0')
            self.host = config.get('data_source', 'host')
            self.port = config.get('data_source', 'port')
            self.service_name = config.get('data_source', 'service_name')
            self.user = config.get('data_source', 'user')
            self.password = config.get('data_source', 'password')
            self.table_name = config.get('data_source', 'table_name')
            # self.table_name = "T_1MINKLINE_BJHY"
            self.to_db_open = bool(config.get('data_source', 'to_db_open'))
            self.tdxdir = config.get('new_tdx', 'tdxdir')
            logger.info(self.service_name)
            logger.info(self.user)
            logger.info(self.tdxdir)
            self.dns = cx_Oracle.makedsn(self.host, self.port, service_name=self.service_name)
            self.reader = Reader.factory(market='std', tdxdir=self.tdxdir)
        except Exception as e:
            logger.exception(e)

    def tdfdt_to_pddt(self, tdfdate, tdftime, use_utc=True, tz_str='+0800'):
        if use_utc:
            merged_datetime = tdfdate + tdftime.zfill(9)
            formated_datetime = merged_datetime[0:4] + '-' + \
                                merged_datetime[4:6] + '-' + \
                                merged_datetime[6:8] + '-' + \
                                merged_datetime[8:10] + '-' + \
                                merged_datetime[10:12] + '-' + \
                                merged_datetime[12:14] + '-' + \
                                merged_datetime[14:17] + ' ' + tz_str
            return pd.to_datetime(formated_datetime, format="%Y-%m-%d-%H-%M-%S-%f %z", utc=True)
        else:
            merged_datetime = tdfdate + tdftime.zfill(9)
            formated_datetime = merged_datetime[0:4] + '-' + \
                                merged_datetime[4:6] + '-' + \
                                merged_datetime[6:8] + '-' + \
                                merged_datetime[8:10] + '-' + \
                                merged_datetime[10:12] + '-' + \
                                merged_datetime[12:14] + '-' + \
                                merged_datetime[14:17]
            return pd.to_datetime(formated_datetime, format="%Y-%m-%d-%H-%M-%S-%f", utc=False)

    def fast_scandir(self, dir, ext):
        subfolders, files = [], []

        for f in os.scandir(dir):
            if f.is_dir():
                subfolders.append(f.path)
            if f.is_file():
                if os.path.splitext(f.name)[1].lower() in ext:
                    files.append(f.path)
        for dir in list(subfolders):
            sf, f = self.fast_scandir(dir, ext)
            subfolders.extend(sf)
            files.extend(f)
        return subfolders, files

    def get_symbols(self, folder):
        subfolders, files = self.fast_scandir(folder, [".lc1"])
        file_list = []
        symbol_list = []
        for f in files:
            fn_full_path = os.path.abspath(f)
            file_list.append(fn_full_path)
            symbol_list.append(os.path.basename(f).split('.')[0])
        return file_list, symbol_list

    def insert_data(self, data):
        if len(data) > 0:
            # 插入数据的SQL语句
            sql = f"INSERT INTO {self.table_name} (TRADEDT ,OPEN, HIGH, LOW, CLOSE, AMOUNT, VOLUME, STOCKCODE)" \
                  f" VALUES (:1, :2,:3,:4,:5,:6,:7,:8)"
            try:
                with cx_Oracle.connect(user=self.user, password=self.password, dsn=self.dns) as conn:
                    cursor = conn.cursor()
                    cursor.executemany(sql, data)
                    cursor.close()
                    conn.commit()
                    logger.info(f"insert-commit:{len(data)}")
            except Exception as e:
                print(f"Insert data failed: {e}")
        else:
            logger.error("no data")

    def read_1min_file(self, s):
        df_klines = self.reader.minute(symbol=s)
        df_klines.index.name = "TRADEDT"
        df_klines['STOCKCODE'] = s[:2].upper() + '.' + s[2:]
        dt_localized = self.date_start.tz_localize(None)
        df_klines = df_klines.loc[dt_localized:]
        logger.info(f"sk-{s}-{df_klines.shape}")
        self.df_list.append(df_klines.reset_index())

    def run(self):
        try:
            files_sz, symbols_sz = self.get_symbols(f'{self.tdxdir}/vipdoc/sz/minline')
            files_sh, symbols_sh = self.get_symbols(f'{self.tdxdir}/vipdoc/sh/minline')
            symbols = symbols_sz + symbols_sh
            logger.info(symbols)
            logger.info(self.table_name)
            with ThreadPoolExecutor(max_workers=10) as executor:
                all_task = [executor.submit(self.read_1min_file, (s)) for s in symbols[:]]

            # for future in as_completed(all_task):
            #     data = future.result()

            if len(self.df_list) > 0:
                if self.to_db_open:
                    df = pd.concat(self.df_list)
                    logger.info(df.shape)
                    data = df.values.tolist()
                    batch_size = 50000
                    futures = []
                    with ThreadPoolExecutor(max_workers=15) as executor:
                        for i in range(0, len(data), batch_size):
                            future = executor.submit(self.insert_data, data[i:i + batch_size])
                            futures.append(future)
                        concurrent.futures.wait(futures)
                    logger.info("done")
        except Exception as e:
            logger.exception(e)


if __name__ == "__main__":
    # database_init()
    mkl = MinkLine()
    mkl.run()
