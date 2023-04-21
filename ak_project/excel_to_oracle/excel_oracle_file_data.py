import cx_Oracle
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
import numpy as np
import traceback
import os
import datetime
from concurrent.futures import ThreadPoolExecutor
import warnings

warnings.filterwarnings("ignore")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='dazh')
engine = create_engine("oracle://fcdb:fcdb@" + dns, echo=True)

product_list = []
concept_list = []


def stock_industry(s):
    code, ex = s.split(".")
    return ex + "." + code


def stock_product(s):
    global product_list
    product = s.split("||")
    product_list.extend(product)
    product_list = list(set(product_list))


def stock_concept(s):
    global concept_list
    concept = s.split(";")
    concept_list.extend(concept)
    concept_list = list(set(concept_list))


def write_stock_industry_info_data(filename, tradedate_str):
    try:
        df = pd.read_excel(filename, sheet_name=0)
        tradedate = datetime.datetime.strptime(tradedate_str, '%Y%m%d')
        # df["ENTRYDATETIME"] = datetime.datetime.now()
        inner = df['所属同花顺行业'].str.split('-', expand=True)
        inner.columns = ["INDLEVEL1NAME",
                         "INDLEVEL2NAME",
                         "INDLEVEL3NAME"]
        df.drop(columns=["所属同花顺行业"], inplace=True)
        df.columns = ["STOCKCODE",
                      "STOCKNAME",
                      "TCLOSE",
                      "PCHG",
                      "MAINPRODUCTS",
                      "BELONGINGCONCEPT",
                      "NATUREOFBUSINESS",
                      "COMPANYWEBSITE",
                      "BELONGINGCONCEPTNUM",
                      "MKTCAP"]
        df = pd.merge(df, inner, how='left', left_index=True, right_index=True)
        df.replace({"TCLOSE": {"--": 0}, "PCHG": {"--": 0}, "MKTCAP": {"--": 0}}, inplace=True)
        df["ENTRYDATETIME"] = tradedate
        df["STOCKCODE"] = df["STOCKCODE"].apply(stock_industry)
        df["TCLOSE"] = df["TCLOSE"].astype(np.float64)
        df["PCHG"] = df["PCHG"].astype(np.float64)
        df["MKTCAP"] = df["MKTCAP"].astype(np.float64)
        with ThreadPoolExecutor(max_workers=3) as pool:
            future1 = pool.submit(read_rep_produc_data, df)
            future2 = pool.submit(write_data, df)
            future3 = pool.submit(read_rep_concept_data, df)
        return "write_stock_industry_info_data finished"
    except Exception as e:
        logger.exception(e)


def write_data(df):
    try:
        df.to_sql("T_S_THS_STOCK_INDUSTRY", engine, index=False, chunksize=1000, if_exists='append')
    except Exception as e:
        logger.exception(e)


def write_industry_index_vol_data(filename, tradedate_str):
    # file_path = os.path.join(path, "20220630-板块-指数-成交量.xlsx")
    names = ["INDEXCODE", "INDEXNAME",
             "TCLOSE",
             "PCHG",
             "INDUSTRYIDXNAME",
             "VOL",
             "TOPEN",
             "QUOTCLOSE",
             "AMOUNT",
             "ACTIVEBUYLARGEORDERS",
             "ACTIVESELLLARGEORDERS",
             "PASSIVEBUYLARGEORDERS",
             "PASSIVESELLLARGEORDERS"]
    df = pd.read_excel(filename, sheet_name=0, names=names)
    tradedate = datetime.datetime.strptime(tradedate_str, '%Y%m%d')
    # df["ENTRYDATETIME"] = datetime.datetime.now()
    df["ENTRYDATETIME"] = tradedate

    df.to_sql("T_S_THS_INDEX", engine, index=False, chunksize=1000, if_exists='append')
    return "write_industry_index_vol_data finished"


def read_rep_produc_data(df):
    global product_list
    try:
        df["MAINPRODUCTS"].apply(stock_product)
        rep_df_list = pd.read_sql(text("select PRODUCTNAME from T_S_THS_MAINPRODUCTS"), engine.connect(),
                                  chunksize=5000)
        dflist = []
        product_set = set(product_list)
        for chunk in rep_df_list:
            dflist.append(chunk)
        rep_df = pd.concat(dflist)
        rep_df_set = set(rep_df.to_dict(orient='list').get("productname"))
        difference = product_set - rep_df_set
        difference_list = list(difference)
        if len(difference_list) > 0:
            df = pd.DataFrame(difference_list, columns=["PRODUCTNAME"])
            df.to_sql("T_S_THS_MAINPRODUCTS", engine, index=False, chunksize=1000, if_exists='append')

    except:
        traceback.print_exc()


def read_rep_concept_data(df):
    global concept_list
    try:
        df["BELONGINGCONCEPT"].apply(stock_concept)
        rep_df_list = pd.read_sql(text("select CONCEPTNAME from T_S_THS_CONCEPT"), engine.connect(), chunksize=5000)
        dflist = []
        concept_set = set(concept_list)
        for chunk in rep_df_list:
            dflist.append(chunk)
        rep_df = pd.concat(dflist)
        rep_df_set = set(rep_df.to_dict(orient='list').get("conceptname"))
        difference = concept_set - rep_df_set
        difference_list = list(difference)
        if len(difference_list) > 0:
            df = pd.DataFrame(difference_list, columns=["CONCEPTNAME"])
            df.to_sql("T_S_THS_CONCEPT", engine, index=False, chunksize=1000, if_exists='append')

    except:
        traceback.print_exc()


def load(stock_industry_info_filename, write_industry_index_vol_filename, tradedate_str):
    current_path = os.path.dirname(os.path.abspath(__file__))
    father_path = os.path.dirname(current_path)
    path = os.path.join(father_path, "excel_data")
    stock_industry_info_path = os.path.join(path, stock_industry_info_filename)
    write_industry_index_vol_path = os.path.join(path, write_industry_index_vol_filename)
    with ThreadPoolExecutor(max_workers=3) as pool:
        future1 = pool.submit(write_stock_industry_info_data, stock_industry_info_path, tradedate_str)
        future2 = pool.submit(write_industry_index_vol_data, write_industry_index_vol_path, tradedate_str)

        def get_result(future):
            print(future.result())

        future1.add_done_callback(get_result)
        future2.add_done_callback(get_result)


def main(tradedate=None):
    if tradedate is None:
        tradedate = datetime.datetime.now().strftime("%Y%m%d")
    # stock_industry_info_filename = f"{tradedate}-股票-板块.xlsx"
    # write_industry_index_vol_filename = f"{tradedate}-板块-指数-成交量.xlsx"
    stock_industry_info_filename = "2023-04-20-所有股票行业分类-主营-概念.xlsx"
    write_industry_index_vol_filename = "2023-04-20-板块指数-成交量-总额.xlsx"
    load(stock_industry_info_filename, write_industry_index_vol_filename, tradedate)


if __name__ == '__main__':
    tradedate = "20230420"
    main(tradedate)
