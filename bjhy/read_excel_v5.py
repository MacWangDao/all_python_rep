import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import paramiko
from loguru import logger

import os
import platform
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler(timezone='Asia/Shanghai')

buffer = BytesIO()


def sftp_upload_xlsx(buffer, remote_path, fordate):
    """
    上传excel文件，流方式
    """
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname='192.168.101.211', username='toptrade', password='toptrade')
        ftp_client = ssh_client.open_sftp()
        xlsm = open("./genRank/股票开闭市涨幅档次排名.xlsx", "wb")
        with ftp_client.open(remote_path + '/股票开闭市涨幅档次排名-' + fordate + '.xlsx', "w") as f:
            xlsm_byes = buffer.getvalue()
            xlsm.write(xlsm_byes)
            f.write(xlsm_byes)
            logger.info('/股票开闭市涨幅档次排名-' + fordate + '.xlsx')
    except Exception as e:
        logger.error(e)


def sftp_download_excel(remote_path):
    host = '192.168.101.211'  # sftp主机
    port = 22  # 端口
    username = 'toptrade'  # sftp用户名
    password = 'toptrade'  # 密码
    sf = paramiko.Transport((host, port))
    sf.connect(username=username, password=password)
    try:
        sftp = paramiko.SFTPClient.from_transport(sf)
        with sftp.open(remote_path) as remote_file:
            df = pd.read_excel(remote_file.read(), sheet_name=None)
            return df
    except Exception as e:
        logger.error(e)
    finally:
        sf.close()


def stock_rank(s):
    y = np.linspace(s.min(), s.max(), 8)
    return pd.cut(x=s, bins=y, right=True, labels=[-3, -2, -1, 0, 1, 2, 3], include_lowest=True)


def generate_byte(df):
    # df = pd.read_excel("./rank/每日板块开盘闭市涨幅排名.xlsm", sheet_name=None)
    # df.pop("0630开盘")
    # df.pop("比较")
    # df.pop("0701开盘")
    # df.pop("0701闭市")
    data_dict = {}
    for k, v in df.items():
        if not data_dict.get(k[:8]):
            data_dict[k[:8]] = [v]
        else:
            data_dict[k[:8]].append(v)
    # print(len(data_dict['0701']))
    # print(data_dict["0801"][1].iloc[:, :2])
    # data_dict["0801"][1].iloc[:, :2].to_csv("./base_industory/base_industory.csv", index=False)
    base_industory = pd.read_csv("./base_industory/base_industory.csv")
    base_industory.set_index(["名称"], drop=True, append=False, inplace=True)
    # print(base_industory)
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
    df = trade.apply(stock_rank)
    df = pd.merge(base_industory, df, how='outer', left_index=True, right_index=True)
    df_columns_list = sorted(df.columns.tolist(), reverse=True)
    df = df.loc[:, df_columns_list]
    df = df.reset_index()
    insert = df["代码"]
    df.drop(labels=["代码"], axis=1, inplace=True)
    df.insert(0, "代码", insert)
    df.sort_values(by=df_columns_list[1], inplace=True, ascending=False)
    # print(df.iloc[:, [1, 0]])
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

    open_gain_rank_style = open_gain_rank.style.background_gradient(subset=open_gain_rank.columns[2:], cmap="RdYlGn",
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

    open_trade = open_trade.apply(stock_rank)
    open_trade = pd.merge(base_industory, open_trade, how='outer', left_index=True, right_index=True)
    open_trade_columns_list = sorted(open_trade.columns.tolist(), reverse=True)
    open_trade = open_trade.loc[:, open_trade_columns_list]
    open_trade = open_trade.reset_index()
    insert = open_trade["代码"]
    open_trade.drop(labels=["代码"], axis=1, inplace=True)
    open_trade.insert(0, "代码", insert)
    close_trade = close_trade.apply(stock_rank)
    close_trade = pd.merge(base_industory, close_trade, how='outer', left_index=True, right_index=True)
    close_trade_columns_list = sorted(close_trade.columns.tolist(), reverse=True)
    close_trade = close_trade.loc[:, close_trade_columns_list]
    close_trade = close_trade.reset_index()
    insert = close_trade["代码"]
    close_trade.drop(labels=["代码"], axis=1, inplace=True)
    close_trade.insert(0, "代码", insert)
    # s = trade["0801闭市"]
    # s.to_csv("./base_industory/base_industory.csv", index=False)
    # y = np.linspace(s.min(), s.max(), 7)
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

    # print(y)
    # print(s.tolist())
    # print(s.tolist()[53])
    # print(s.tolist()[54])
    # print(s.tolist()[55])

    # b = pd.cut(x=s, bins=y, right=True, labels=[-3, -2, -1, 0, 1, 2], include_lowest=True)
    # print(b.tolist())
    # b.to_csv("tmp110.csv")
    # b = pd.cut(x=s, bins=y, right=True)
    # print(b.tolist()[53])
    # print(b.tolist()[54])
    # print(b.tolist()[55])

    # print(trade.rank(method='min', ascending=False))
    # y = np.linspace(lowlimited, highlimited, y)
    # df_record['price_range'] = pd.cut(x=df_record["price"], bins=y, right=True)
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        trade.to_excel(writer,
                       sheet_name='开闭市涨幅',
                       merge_cells=False, index=False)
        df.to_excel(writer,
                    sheet_name='开闭市档次',
                    merge_cells=False, index=False)
        rank.to_excel(writer,
                      sheet_name='开闭市排名',
                      merge_cells=False, index=False)
        open_trade.style.background_gradient(subset=open_trade.columns[2:],
                                             cmap="RdYlGn",
                                             high=0.2,
                                             low=0.1).to_excel(writer,
                                                               sheet_name='开市档次',
                                                               merge_cells=False, index=False)
        close_trade.style.background_gradient(subset=close_trade.columns[2:],
                                              cmap="RdYlGn",
                                              high=0.2,
                                              low=0.1).to_excel(writer,
                                                                sheet_name='闭市档次',
                                                                merge_cells=False, index=False)
        open_gain_rank_style.to_excel(writer,
                                      sheet_name='开市涨幅排名',
                                      merge_cells=False, index=False)
        close_gain_rank_style.to_excel(writer,
                                       sheet_name='闭市涨幅排名',
                                       merge_cells=False, index=False)
        open_rank_style.to_excel(writer,
                                 sheet_name='开市排名',
                                 merge_cells=False, index=True)
        close_rank_style.to_excel(writer,
                                  sheet_name='闭市排名',
                                  merge_cells=False, index=True)


def main():
    fordate = datetime.datetime.now().strftime("%m%d")
    remote_path = "/home/guest/003-数据/005-其他需求/每日板块开盘闭市涨幅排名.xlsm"
    df = sftp_download_excel(remote_path)
    generate_byte(df)
    remote_path = "/home/guest/003-数据/005-其他需求"
    sftp_upload_xlsx(buffer, remote_path, fordate)


@scheduler.scheduled_job("cron", day_of_week="0-4", hour="15", minute="12,15,20,30,40", id='task1', max_instances=3)
def job_first():
    main()


if __name__ == "__main__":
    main()
