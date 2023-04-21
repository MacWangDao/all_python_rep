import datetime
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import concurrent.futures
import pandas as pd
import paramiko
from loguru import logger
from mootdx.reader import Reader
import configparser
import os
import warnings

warnings.filterwarnings("ignore")
logger.add("1minkline_to_sftp_csv.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class DailyMinkLineToCsv(object):
    def __init__(self, config_path: str = "config_sftp_csv.ini"):
        try:
            self.df_list = []
            logger.info("config_sftp_csv.ini")
            config = configparser.ConfigParser()
            config.read(config_path, encoding='utf-8')
            now_date_time = config.get('date', 'now_date_time')
            if len(now_date_time) == 0:
                now_date_time = datetime.datetime.today().strftime("%Y%m%d")
            # now_date_time = "20230227"
            self.now_date_time = now_date_time
            logger.info(now_date_time)
            self.date_start = self.tdfdt_to_pddt(now_date_time, '0')
            self.host = config.get('sftp', 'host')
            self.port = config.get('sftp', 'port')
            self.remote_path = config.get('sftp', 'sftp_path')
            logger.info(self.remote_path)
            self.username = config.get('sftp', 'user')
            self.password = config.get('sftp', 'password')
            self.tdxdir = config.get('new_tdx', 'tdxdir')
            self.check_file = bool(config.get('sftp', 'check_file'))
            logger.info(self.tdxdir)
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

    def sftp_download_csvfile(self):
        sf = paramiko.Transport((self.host, int(self.port)))
        sf.connect(username=self.username, password=self.password)
        try:
            sftp = paramiko.SFTPClient.from_transport(sf)
            self.remote_files = sftp.listdir(self.remote_path)
            self.sk_codes = [sk[:9] for sk in self.remote_files]
        except Exception as e:
            logger.exception(e)
        finally:
            sf.close()

    def read_1min_file(self, s):
        sf = None
        try:
            df_klines = self.reader.minute(symbol=s)
            df_klines.index.name = "trade_time"
            stockcode = f"{s[2:]}.{s[:2].upper()}"
            dt_localized = self.date_start.tz_localize(None)
            df_klines = df_klines.loc[dt_localized:]
            # logger.info(f"sk-{s}-{df_klines.shape}")
            df_klines.reset_index(inplace=True)
            df_klines.rename(columns={"volume": "vol"}, inplace=True)
            df_klines = df_klines.reindex(columns=["trade_time", "open", "high", "low", "close", "vol", "amount"])
            df_klines["amount"] = df_klines["amount"].astype(int)
            df_klines = df_klines.round({"open": 2, "high": 2, "low": 2, "close": 2, "vol": 0, "amount": 0})
            if not df_klines.empty:
                sf = paramiko.Transport((self.host, int(self.port)))
                sf.connect(username=self.username, password=self.password)
                file_path_name = f"{self.remote_path}/{stockcode}.csv"
                sftp = paramiko.SFTPClient.from_transport(sf)
                if self.check_file:
                    with sftp.open(file_path_name) as remote_file:
                        df = pd.read_csv(remote_file, usecols=["trade_time"])
                        if df["trade_time"].max() != df_klines["trade_time"].max().strftime("%Y-%m-%d %H:%M:%S"):
                            with sftp.open(file_path_name, 'a') as remote_file:
                                df_klines.to_csv(remote_file, index=False, header=False)
                                logger.info(f"upload:{stockcode}-{df_klines.shape}")
                        else:
                            logger.warning(f"upload:{stockcode}-data exist")
                else:
                    with sftp.open(file_path_name, 'a') as remote_file:
                        df_klines.to_csv(remote_file, index=False, header=False)
                        logger.info(f"upload:{stockcode}-{df_klines.shape}")
            else:
                logger.warning(f"sk:{stockcode}-{df_klines.shape}")

        except Exception as e:
            logger.exception(e)
        finally:
            if sf is not None:
                sf.close()

    def run(self):
        try:
            files_sz, symbols_sz = self.get_symbols(f'{self.tdxdir}/vipdoc/sz/minline')
            files_sh, symbols_sh = self.get_symbols(f'{self.tdxdir}/vipdoc/sh/minline')
            symbols = symbols_sz + symbols_sh
            # logger.info(symbols)
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.host, username=self.username, password=self.password)
            stdin, stdout, stderr = ssh_client.exec_command(
                f"echo 'toptrade' | sudo -S chmod 777 {self.remote_path} -R", timeout=300)
            out = stdout.readlines()
            self.sftp_download_csvfile()
            futures = []
            # ThreadPoolExecutor
            # ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=8) as executor:
                for s in symbols[:]:
                    if f"{s[2:]}.{s[:2].upper()}" in self.sk_codes:
                        future = executor.submit(self.read_1min_file, s)
                        futures.append(future)
                concurrent.futures.wait(futures)
            logger.info("done")
            self.send_check_file()
        except Exception as e:
            logger.exception(e)

    def send_check_file(self):
        self.sftp_download_csvfile()
        futures = []
        with ProcessPoolExecutor(max_workers=10) as executor:
            for file_name in self.remote_files[:]:
                future = executor.submit(self.check_file_sftp, file_name)
                futures.append(future)
            concurrent.futures.wait(futures)

    def check_file_sftp(self, file_name: str = None):
        sf = None
        try:
            sf = paramiko.Transport((self.host, int(self.port)))
            sf.connect(username=self.username, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(sf)
            file_patn_name = f"{self.remote_path}/{file_name}"
            dt_localized = pd.to_datetime(self.now_date_time)
            with sftp.open(file_patn_name) as remote_file:
                df = pd.read_csv(remote_file, usecols=["trade_time"])
                df["trade_time"] = pd.to_datetime(df["trade_time"])
                df.set_index("trade_time", inplace=True, drop=False)
                df_exists = df.loc[dt_localized:]
                x, y = df_exists.shape
                if x == 0:
                    logger.warning(f"{file_name}-shape:{df_exists.shape}")
                else:
                    logger.info(f"{file_name}-{self.now_date_time}-success")
        except Exception as e:
            logger.exception(e)
        finally:
            if sf is not None:
                sf.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    mkl = DailyMinkLineToCsv()
    mkl.run()
