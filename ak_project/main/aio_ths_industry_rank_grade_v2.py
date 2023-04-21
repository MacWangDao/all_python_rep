import asyncio
import datetime
import functools
import traceback
from io import BytesIO

import aiohttp
import numpy as np
import pandas as pd
import paramiko
import py_mini_racer
from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger
import pytz
import os

# print(pytz.country_timezones('cn'))
# shanghai = pytz.timezone('Asia/Shanghai')
# scheduler = BlockingScheduler(timezone=shanghai)
scheduler = BlockingScheduler(timezone='Asia/Shanghai')


class THS_RankGrede:
    def __init__(self, url, find_date):
        self.url = url
        self.find_date = find_date
        self.buffer = BytesIO()
        self.buffer_xlsm = BytesIO()

    def sftp_upload_xlsx(self, remote_path, fordate):
        """
        上传excel文件，流方式
        """
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname='192.168.101.211', username='toptrade', password='toptrade')
            ftp_client = ssh_client.open_sftp()
            xlsx = open("./xlsx/" + '/股票开闭市涨幅档次排名-' + fordate + '.xlsx', "wb")
            with ftp_client.open(remote_path + '/股票开闭市涨幅档次排名-' + fordate + '.xlsx', "w") as f:
                xlsx_byes = self.buffer.getvalue()
                xlsx.write(xlsx_byes)
                f.write(xlsx_byes)
                logger.info('股票开闭市涨幅档次排名-' + fordate + '.xlsx')
        except Exception as e:
            logger.error(e)

    def sftp_download_excel(self, remote_path):
        host = '192.168.101.211'  # sftp主机
        port = 22  # 端口
        username = 'toptrade'  # sftp用户名
        password = 'toptrade'  # 密码
        sf = paramiko.Transport((host, port))
        sf.connect(username=username, password=password)
        try:
            sftp = paramiko.SFTPClient.from_transport(sf)
            with sftp.open(remote_path) as remote_file:
                # remote_file.flush()
                df = pd.read_excel(remote_file.read(), sheet_name=None)
                return df
        except Exception as e:
            logger.error(e)
        finally:
            sf.close()

    def stock_rank(self, s):
        y = np.linspace(s.min(), s.max(), 8)
        return pd.cut(x=s, bins=y, right=True, labels=[-3, -2, -1, 0, 1, 2, 3], include_lowest=True)

    def generate_byte(self, df):
        data_dict = {}
        for k, v in df.items():
            if not data_dict.get(k[:8]):
                data_dict[k[:8]] = [v]
            else:
                data_dict[k[:8]].append(v)

        base_industory = pd.read_csv("./base_industory/base_industory.csv")
        base_industory.set_index(["名称"], drop=True, append=False, inplace=True)
        trade = pd.DataFrame(columns=["名称"])
        trade.set_index(["名称"], drop=True, append=False, inplace=True)
        open_trade = pd.DataFrame(columns=["名称"])
        open_trade.set_index(["名称"], drop=True, append=False, inplace=True)
        close_trade = pd.DataFrame(columns=["名称"])
        close_trade.set_index(["名称"], drop=True, append=False, inplace=True)
        for trade_date, value in data_dict.items():
            if len(value) == 2:
                df1 = value[0]
                df1 = df1.iloc[:, 1:3]
                if not df1.empty:
                    df1.columns = ["名称", trade_date + "开盘"]
                    df1.set_index(["名称"], drop=True, append=False, inplace=True)
                    df1.replace({trade_date + "开盘": {"--": 0}}, inplace=True)
                df2 = value[1]
                df2 = df2.iloc[:, 1:3]
                if not df2.empty:
                    df2.columns = ["名称", trade_date + "闭市"]
                    df2.set_index(["名称"], drop=True, append=False, inplace=True)
                    df2.replace({trade_date + "闭市": {"--": 0}}, inplace=True)
                    trade_df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)
                    trade = pd.merge(trade, trade_df, how='outer', left_index=True, right_index=True)
                    open_trade = pd.merge(open_trade, df1, how='outer', left_index=True, right_index=True)
                    close_trade = pd.merge(close_trade, df2, how='outer', left_index=True, right_index=True)
        df = trade.apply(self.stock_rank)
        df = pd.merge(base_industory, df, how='outer', left_index=True, right_index=True)
        df_columns_list = sorted(df.columns.tolist(), reverse=True)
        df = df.loc[:, df_columns_list]
        df = df.reset_index()
        insert = df["代码"]
        df.drop(labels=["代码"], axis=1, inplace=True)
        df.insert(0, "代码", insert)
        df.sort_values(by=df_columns_list[1], inplace=True, ascending=False)
        open_gain_rank = open_trade.copy()
        columns = [col[:8] for col in open_gain_rank.columns]
        open_gain_rank.columns = columns
        open_gain_rank['平均值'] = open_gain_rank.mean(axis=1)
        open_gain_rank.sort_values(by=["平均值"], inplace=True, ascending=False)
        open_gain_rank = pd.merge(base_industory, open_gain_rank, how='outer', left_index=True, right_index=True)
        open_gain_rank_columns_list = sorted(open_gain_rank.columns.tolist(), reverse=True)
        open_gain_rank = open_gain_rank.loc[:, open_gain_rank_columns_list]
        open_gain_rank = open_gain_rank.reset_index()
        insert = open_gain_rank["代码"]
        open_gain_rank.drop(labels=["代码"], axis=1, inplace=True)
        open_gain_rank.insert(0, "代码", insert)
        open_gain_rank.sort_values(by=["平均值"], inplace=True, ascending=False)
        open_rank = open_gain_rank.copy()
        open_rank.drop(labels=["平均值"], axis=1, inplace=True)
        open_rank.set_index(["代码", "名称"], drop=True, append=False, inplace=True)
        open_rank = open_rank.rank(method='min', ascending=False)
        open_rank['平均排名'] = open_rank.mean(axis=1).round(2)
        open_rank.sort_values(by=["平均排名"], inplace=True, ascending=False)
        insert = open_rank["平均排名"]
        open_rank.drop(labels=["平均排名"], axis=1, inplace=True)
        open_rank.insert(0, "平均排名", insert)

        open_gain_rank_style = open_gain_rank.style.background_gradient(subset=open_gain_rank.columns[2:],
                                                                        cmap="RdYlGn",
                                                                        high=0.2,
                                                                        low=0.1)
        open_rank_style = open_rank.style.background_gradient(subset=open_rank.columns[:], cmap="RdYlGn",
                                                              high=0.2,
                                                              low=0.1)
        close_gain_rank = close_trade.copy()
        columns = [col[:8] for col in close_gain_rank.columns]
        close_gain_rank.columns = columns
        close_gain_rank['平均值'] = close_gain_rank.mean(axis=1)
        close_gain_rank.sort_values(by=["平均值"], inplace=True, ascending=False)
        close_gain_rank = pd.merge(base_industory, close_gain_rank, how='outer', left_index=True, right_index=True)
        close_gain_rank_columns_list = sorted(close_gain_rank.columns.tolist(), reverse=True)
        close_gain_rank = close_gain_rank.loc[:, close_gain_rank_columns_list]
        close_gain_rank = close_gain_rank.reset_index()
        insert = close_gain_rank["代码"]
        close_gain_rank.drop(labels=["代码"], axis=1, inplace=True)
        close_gain_rank.insert(0, "代码", insert)
        close_gain_rank.sort_values(by=["平均值"], inplace=True, ascending=False)
        close_rank = close_gain_rank.copy()
        close_rank.drop(labels=["平均值"], axis=1, inplace=True)
        close_rank.set_index(["代码", "名称"], drop=True, append=False, inplace=True)
        close_rank = close_rank.rank(method='min', ascending=False)
        close_rank['平均排名'] = close_rank.mean(axis=1).round(2)
        close_rank.sort_values(by=["平均排名"], inplace=True, ascending=False)
        insert = close_rank["平均排名"]
        close_rank.drop(labels=["平均排名"], axis=1, inplace=True)
        close_rank.insert(0, "平均排名", insert)
        close_gain_rank_style = close_gain_rank.style.background_gradient(subset=close_gain_rank.columns[2:],
                                                                          cmap="RdYlGn",
                                                                          high=0.2,
                                                                          low=0.1)
        close_rank_style = close_rank.style.background_gradient(subset=close_rank.columns[:],
                                                                cmap="RdYlGn",
                                                                high=0.2,
                                                                low=0.1)
        open_trade = open_trade.apply(self.stock_rank)
        open_trade = pd.merge(base_industory, open_trade, how='outer', left_index=True, right_index=True)
        open_trade_columns_list = sorted(open_trade.columns.tolist(), reverse=True)
        open_trade = open_trade.loc[:, open_trade_columns_list]
        open_trade = open_trade.reset_index()
        insert = open_trade["代码"]
        open_trade.drop(labels=["代码"], axis=1, inplace=True)
        open_trade.insert(0, "代码", insert)
        close_trade = close_trade.apply(self.stock_rank)
        close_trade = pd.merge(base_industory, close_trade, how='outer', left_index=True, right_index=True)
        close_trade_columns_list = sorted(close_trade.columns.tolist(), reverse=True)
        close_trade = close_trade.loc[:, close_trade_columns_list]
        close_trade = close_trade.reset_index()
        insert = close_trade["代码"]
        close_trade.drop(labels=["代码"], axis=1, inplace=True)
        close_trade.insert(0, "代码", insert)
        rank = trade.rank(method='min', ascending=False)
        rank = pd.merge(base_industory, rank, how='outer', left_index=True, right_index=True)
        rank_columns_list = sorted(rank.columns.tolist(), reverse=True)
        rank = rank.loc[:, rank_columns_list]
        rank = rank.reset_index()
        insert = rank["代码"]
        rank.drop(labels=["代码"], axis=1, inplace=True)
        rank.insert(0, "代码", insert)
        rank.sort_values(by=rank_columns_list[1], inplace=True, ascending=True)
        trade = pd.merge(base_industory, trade, how='outer', left_index=True, right_index=True)
        trad_columns_list = sorted(trade.columns.tolist(), reverse=True)
        trade = trade.loc[:, trad_columns_list]
        trade = trade.reset_index()
        insert = trade["代码"]
        trade.drop(labels=["代码"], axis=1, inplace=True)
        trade.insert(0, "代码", insert)
        trade.sort_values(by=trad_columns_list[1], inplace=True, ascending=False)
        with pd.ExcelWriter(self.buffer, engine='xlsxwriter') as writer:
            trade.to_excel(writer,
                           sheet_name='开闭市涨幅',
                           encoding="utf-8",
                           merge_cells=False, index=False)
            df.to_excel(writer,
                        sheet_name='开闭市档次',
                        encoding="utf-8",
                        merge_cells=False, index=False)
            rank.to_excel(writer,
                          sheet_name='开闭市排名',
                          encoding="utf-8",
                          merge_cells=False, index=False)
            open_trade.style.background_gradient(subset=open_trade.columns[2:],
                                                 cmap="RdYlGn",
                                                 high=0.2,
                                                 low=0.1).to_excel(writer,
                                                                   sheet_name='开市档次',
                                                                   encoding="utf-8",
                                                                   merge_cells=False, index=False)
            close_trade.style.background_gradient(subset=close_trade.columns[2:],
                                                  cmap="RdYlGn",
                                                  high=0.2,
                                                  low=0.1).to_excel(writer,
                                                                    sheet_name='闭市档次',
                                                                    encoding="utf-8",
                                                                    merge_cells=False, index=False)
            open_gain_rank_style.to_excel(writer,
                                          sheet_name='开市涨幅排名',
                                          encoding="utf-8",
                                          merge_cells=False, index=False)
            close_gain_rank_style.to_excel(writer,
                                           sheet_name='闭市涨幅排名',
                                           encoding="utf-8",
                                           merge_cells=False, index=False)
            open_rank_style.to_excel(writer,
                                     sheet_name='开市排名',
                                     encoding="utf-8",
                                     merge_cells=False, index=True)
            close_rank_style.to_excel(writer,
                                      sheet_name='闭市排名',
                                      encoding="utf-8",
                                      merge_cells=False, index=True)

    def sftp_upload_xlsm(self, remote_path):
        """
        上传excel文件，流方式
        """
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname='192.168.101.211', username='toptrade', password='toptrade')
            stdin, stdout, stderr = ssh_client.exec_command(
                "echo 'toptrade' | sudo -S chmod 777 /home/guest/003-数据/005-其他需求 -R", timeout=300)
            out = stdout.readlines()
            # print(out)
            ftp_client = ssh_client.open_sftp()
            xlsm = open("./xlsm/股票开闭市涨幅档次排名.xlsm", "wb")
            with ftp_client.open(remote_path, "w") as f:
                xlsm_byes = self.buffer_xlsm.getvalue()
                xlsm.write(xlsm_byes)
                xlsm.flush()
                f.write(xlsm_byes)
                f.flush()
                logger.info('股票开闭市涨幅档次排名')
        except Exception as e:
            logger.error(e)

    def ths_pgh_update(self, ths_data):
        try:
            hours = datetime.datetime.now().hour
            fordate = datetime.datetime.now().strftime("%Y%m%d")
            close = 0
            if hours >= 15:
                status = f'{fordate}闭市'
                close = 1
            else:
                status = f'{fordate}开盘'
            remote_path = "/home/guest/003-数据/005-其他需求/每日板块开盘闭市涨幅排名/每日板块开盘闭市涨幅排名.xlsm"
            df_set = self.sftp_download_excel(remote_path)
            with pd.ExcelWriter(self.buffer_xlsm, engine='xlsxwriter') as writer:
                for k, v in df_set.items():
                    v.to_excel(writer,
                               sheet_name=k,
                               encoding="utf-8",
                               merge_cells=False, index=False)
                ths_data.to_excel(writer,
                                  sheet_name=status,
                                  encoding="utf-8",
                                  merge_cells=False, index=False)
                workbook = writer.book
                workbook.add_vba_project('./vba/open_close/vbaProject.bin')
                cell_format = workbook.add_format({'font_name': '等线', 'font_size': 12})
                worksheet = workbook.worksheets()[-1]
                worksheet.set_column('A:H', 10, cell_format=cell_format)
                worksheet.activate()
                # worksheet.select()
                workbook.worksheets()[-11].set_first_sheet()
                # cell_format = workbook.add_format({'font_name': 'Arial', 'font_size': 10})
                # cell_format.set_font_name('等线')
            self.sftp_upload_xlsm(remote_path)
            if close == 1:
                df = pd.read_excel(self.buffer_xlsm.getvalue(), sheet_name=None)
                self.generate_byte(df)
                remote_path = "/home/guest/003-数据/005-其他需求/每日板块开盘闭市涨幅排名"
                self.sftp_upload_xlsx(remote_path, fordate)
            return ths_data
        except:
            traceback.print_exc()
        finally:
            self.buffer.close()
            self.buffer_xlsm.close()

    def _get_file_content_ths(self, file: str = "ths.js") -> str:
        current_path = os.path.dirname(os.path.abspath(__file__))
        # father_path = os.path.dirname(current_path)
        path = os.path.join(current_path, file)

        """
        获取 JS 文件的内容
        :param file:  JS 文件名
        :type file: str
        :return: 文件内容
        :rtype: str
        """

        with open(path) as f:
            file_data = f.read()
        return file_data

    def save_df_data(self, data_json):
        data = data_json["data"]["answer"][0]["txt"][0]["content"]["components"][0]["data"]["datas"]
        temp_df = pd.DataFrame(
            data
        )
        temp_df.drop(
            columns=["指数@同花顺行业指数", "指数@所属同花顺行业级别", "market_code", f"指数@最高价:不复权[{self.find_date}]",
                     f"指数@换手率[{self.find_date}]",
                     "指数代码",
                     f"指数@成交量[{self.find_date}]"],
            inplace=True, errors="ignore")
        temp_df.rename(columns={
            "code": "代码",
            "指数简称": "名称",
            f"指数@成交额[{self.find_date}]": "总金额",
            f"指数@总市值[{self.find_date}]": "总市值",
            f"指数@流通市值[{self.find_date}]": "流通市值",
            f"指数@涨跌幅:前复权[{self.find_date}]": "涨幅",
            f"指数@开盘价:不复权[{self.find_date}]": "开盘价",
            f"指数@收盘价:不复权[{self.find_date}]": "现价"

        }, inplace=True)
        # temp_df.drop(
        #     columns=["现价"],
        #     inplace=True, errors="ignore")
        if "现价" in temp_df.columns:
            df = temp_df.loc[:, ["代码", "名称", "涨幅", "现价", "开盘价", "总金额", "总市值", "流通市值"]].copy()
        else:
            df = temp_df.loc[:, ["代码", "名称", "涨幅", "开盘价", "总金额", "总市值", "流通市值"]].copy()
            df["现价"] = df["开盘价"]
            df = df.loc[:, ["代码", "名称", "涨幅", "现价", "开盘价", "总金额", "总市值", "流通市值"]]
        df["涨幅"] = df["涨幅"].astype(np.float64)
        df["现价"] = df["现价"].astype(np.float64)
        df["开盘价"] = df["开盘价"].astype(np.float64)
        df["总金额"] = df["总金额"].astype(np.float64)
        df["总市值"] = df["总市值"].astype(np.float64)
        df["流通市值"] = df["流通市值"].astype(np.float64)
        return df

    @logger.catch
    def call_back_ths_industry(self, task):
        if task.result() is None:
            logger.error(f"task:{None}")
        else:
            jdata, status = task.result()
            if status in [200, 201]:
                try:
                    ths_data = self.save_df_data(jdata)
                    self.ths_pgh_update(ths_data)
                except Exception as e:
                    logger.error(e)
            else:
                logger.error("status:ERROR")

    @logger.catch
    async def fetch_ths(self, session, url, headers, data, ip=None):
        try:
            await asyncio.sleep(0.5)
            async with session.post(url, data=data, headers=headers, verify_ssl=False, timeout=10,
                                    proxy=None) as response:
                return await response.json(), response.status
        except Exception as e:
            print(e)

    async def network_loop_ths_industry(self):
        tasks = []
        js_code = py_mini_racer.MiniRacer()
        js_content = self._get_file_content_ths("ths.js")
        js_code.eval(js_content)

        try:
            connector = aiohttp.TCPConnector(ssl=False, limit=0)
            async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
                v_code = js_code.call("v")
                headers = {
                    "hexin-v": v_code,
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
                }
                data = {
                    "question": f"{self.find_date} 二级行业,涨幅,流通股",
                    "perpage": "100",
                    "page": "1",
                    "secondary_intent": "zhishu",
                    "log_info": '{"input_type":"typewrite"}',
                    "source": "Ths_iwencai_Xuangu",
                    "version": "2.0",
                    "query_area": "",
                    "block_list": "",
                    "add_info": '{"urp":{"scene":1,"company":1,"business":1},"contentType":"json","searchInfo":true}',
                }
                await asyncio.sleep(1)
                task = asyncio.ensure_future(self.fetch_ths(session, self.url, headers, data))
                task.add_done_callback(functools.partial(self.call_back_ths_industry))
                tasks.append(task)
                await asyncio.wait(tasks)
        except Exception as e:
            logger.error(e)

    async def run(self):
        task1 = asyncio.ensure_future(self.network_loop_ths_industry())
        await task1


def ths_starat():
    url = "http://www.iwencai.com/unifiedwap/unified-wap/v2/result/get-robot-data"
    find_date: str = datetime.datetime.now().strftime("%Y%m%d")
    thsrg1 = THS_RankGrede(url, find_date)
    asyncio.run(thsrg1.run())


# @scheduler.scheduled_job("cron", day_of_week="0-4", hour="9", minute="28", second="5", id='task101', max_instances=3,
#                          misfire_grace_time=10, timezone=shanghai)
@scheduler.scheduled_job("cron", day_of_week="0-4", hour="9", minute="30", second="5", id='task103', max_instances=3,
                         misfire_grace_time=10)
def job_first():
    ths_starat()


# @scheduler.scheduled_job("cron", day_of_week="0-4", hour="15", minute="1", second="5", id='task102', max_instances=3,
#                          misfire_grace_time=10, timezone=shanghai)
@scheduler.scheduled_job("cron", day_of_week="0-4", hour="15", minute="1", second="5", id='task102', max_instances=3,
                         misfire_grace_time=10)
def job_second():
    ths_starat()


if __name__ == '__main__':
    # ths_starat()
    scheduler.start()
