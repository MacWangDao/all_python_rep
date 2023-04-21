import datetime
import os

import pandas as pd
from loguru import logger
from mootdx.reader import Reader
from sqlalchemy import create_engine
import configparser
import cx_Oracle

logger.add("1minkline_to_oracle.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class MinkLine(object):
    def __init__(self, config_path: str = "config.ini"):
        try:
            logger.info("config.ini")
            config = configparser.ConfigParser()
            config.read(config_path, encoding='utf-8')
            now_date_time = config.get('date', 'now_date_time')
            if len(now_date_time) == 0:
                now_date_time = datetime.datetime.today().strftime("%Y%m%d")
            logger.info(now_date_time)
            self.date_start = self.tdfdt_to_pddt(now_date_time, '0')
            oracle_source = config.get('data_source', 'oracle_source')
            self.table_name = config.get('data_source', 'table_name')
            self.to_db_open = bool(config.get('data_source', 'to_db_open'))
            if len(oracle_source) == 0:
                oracle_source = "oracle+cx_oracle://fcdb:fcdb@dazh"
            logger.info(oracle_source)
            self.tdxdir = config.get('new_tdx', 'tdxdir')
            logger.info(self.tdxdir)
            dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='dazh')  # dsn
            self.engine = create_engine("oracle://fcdb:fcdb@" + dns, encoding='utf-8', echo=False, pool_size=0, max_overflow=-1)
            # self.engine = create_engine("oracle+cx_oracle://fcdb:fcdb@dazh", encoding='utf-8', echo=False)
            # self.engine = create_engine(oracle_source, echo=False)
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

    def run(self):
        try:
            files_sz, symbols_sz = self.get_symbols(f'{self.tdxdir}/vipdoc/sz/minline')
            files_sh, symbols_sh = self.get_symbols(f'{self.tdxdir}/vipdoc/sh/minline')
            symbols = symbols_sz + symbols_sh
            logger.info(symbols)
            print(self.table_name)
            progress = 0
            for s in symbols:
                logger.info(s)
                df_klines = self.reader.minute(symbol=s)
                df_klines.index.name = "TRADEDT"
                df_klines['STOCKCODE'] = s[:2].upper() + '.' + s[2:]
                dt_localized = self.date_start.tz_localize(None)
                df_klines = df_klines.loc[dt_localized:]
                logger.info(df_klines.shape)
                logger.info("inserting : " + s)
                if self.to_db_open:
                    # df_klines.to_sql('t_s_1minkline', conn, if_exists='append')
                    df_klines.to_sql(self.table_name, self.engine, chunksize=500, if_exists='append')
                progress += 1
                if progress % 100 == 0:
                    logger.info("progress : {}".format(progress))
            logger.info("done")
        except Exception as e:
            logger.exception(e)


if __name__ == "__main__":
    mkl = MinkLine()
    mkl.run()
