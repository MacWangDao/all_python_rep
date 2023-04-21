import cx_Oracle
import numpy
import pandas as pd
from sqlalchemy import create_engine
import os
import re
import numpy as np
import traceback

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=True)


def getHingtPath(path):
    flist = list()
    for root, dirs, files in os.walk(path):
        for file in files:
            flist.append(os.path.join(root, file))
    return flist


if __name__ == '__main__':
    try:
        flist = getHingtPath("/data/yu/forcsv")
        path_list = []
        for flis in flist:
            res = re.findall(r".*/SnapShotsh60.*|.*/SnapShotsz30.*|.*/SnapShotsz000.*", flis)
            if res:
                path_list.extend(res)
        code_list = []
        for path in path_list:
            print(path)
            df = pd.read_csv(path, nrows=1, usecols=["date", "code", "highlimited", "lowlimited", "status"])
            df.rename(columns={'date': 'trade_date'}, inplace=True)
            df = df[(df.code.astype(str).str.len() == 6)]
            if len(df) == 1:
                code_list.append(df)
            else:
                continue




        try:
            rep_df_list = pd.read_sql("select trade_date,code from T_O_SNAPSHOT_VALUE", engine, chunksize=50000)
            dflist = []
            for chunk in rep_df_list:
                dflist.append(chunk)
            rep_df = pd.concat(dflist)
            df = pd.concat(code_list, ignore_index=True)
            df["code"] = df["code"].astype(np.str_)
            df["trade_date"] = df["trade_date"].astype(np.str_)
            for index, row in rep_df.iterrows():
                df.drop(index=(df.loc[(df['trade_date'] == row[0]) & (df['code'] == row[1])].index), inplace=True)
            df.to_sql("T_O_SNAPSHOT_VALUE", engine, index=False, chunksize=10000, if_exists='append')
        except:
            traceback.print_exc()
    except:
        traceback.print_exc()
