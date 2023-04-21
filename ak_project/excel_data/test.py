import datetime

import numpy as np
import pandas as pd
import warnings

warnings.filterwarnings('ignore')


# df = pd.read_html("2022-07-04-所有股票行业分类-主营-概念.xls", header=0)
# df = df[0]
# inner = df['所属同花顺行业'].str.split('-', expand=True)
# inner.columns = ["INDEVEL1NAME",
#                  "INDEVEL2NAME",
#                  "INDEVEL3NAME"]
# df.drop(columns=["所属同花顺行业"], inplace=True)
# df.columns = ["STOCKCODE",
#               "STOCKNAME",
#               "TCLOSE",
#               "PCHG",
#               "MAINPRODUCTS",
#               "BELONGINGCONCEPT",
#               "NATUREOFBUSINESS",
#               "COMPANYWEBSITE",
#               "BELONGINGCONCEPTNUM",
#               "MKTCAP"]
# df = pd.merge(df, inner, how='left', left_index=True, right_index=True)
# df.replace({"TCLOSE": {"--": 0}, "PCHG": {"--": 0}, "MKTCAP": {"--": 0}}, inplace=True)
# # df["ENTRYDATETIME"] = tradedate
# # df["STOCKCODE"] = df["STOCKCODE"].apply(stock_industry)
# df["TCLOSE"] = df["TCLOSE"].astype(np.float64)
# df["PCHG"] = df["PCHG"].astype(np.float64)
# df["MKTCAP"] = df["MKTCAP"].astype(np.float64)
# print(df)
# "CODE" VARCHAR2(10),
#    "STOCKCODE" VARCHAR2(10),
# 	"STOCKNAME" VARCHAR2(100),
# --	"TCLOSE" NUMBER,
# --	"PCHG" NUMBER,
# 	"INDLEVEL1CODE" VARCHAR2(10),
# 	"INDLEVEL1NAME" VARCHAR2(100),
# 	"INDLEVEL2CODE" VARCHAR2(10),
# 	"INDLEVEL2NAME" VARCHAR2(100),
# 	"INDLEVEL3CODE" VARCHAR2(10),
# 	"INDLEVEL3NAME" VARCHAR2(100),
# 	"INDLEVEL4CODE" VARCHAR2(10),
# 	"INDLEVEL4NAME" VARCHAR2(100),
# 	"CSRCINDUCATEGCODE" VARCHAR2(10),
# 	"CSRCINDUCATEGNAME" VARCHAR2(100),
# 	"CSRCINDUTYCODE" VARCHAR2(10),
# 	"CSRCINDUTYNAME" VARCHAR2(100),
# 	"ENTRYDATETIME" TIMESTAMP (6)
def stock_code(s):
    cx = ""
    if len(s.split(".")) == 2:
        code, ex = s.split(".")
        cx = ex + "." + code
    else:
        if s.startswith("00"):
            cx = "SZ" + "." + s
        elif s.startswith("30"):
            cx = "SZ" + "." + s
        elif s.startswith("6"):
            cx = "SH" + "." + s

    return cx


df = pd.read_excel("中证行业分类.xlsx", sheet_name=0)
df.columns = ["CODE",
              "STOCKNAME",
              "INDLEVEL1CODE",
              "INDLEVEL1NAME",
              "INDLEVEL2CODE",
              "INDLEVEL2NAME",
              "INDLEVEL3CODE",
              "INDLEVEL3NAME",
              "INDLEVEL4CODE",
              "INDLEVEL4NAME",
              "CSRCINDUCATEGCODE",
              "CSRCINDUCATEGNAME",
              "CSRCINDUTYCODE",
              "CSRCINDUTYNAME"]
df["STOCKCODE"] = df["CODE"].apply(stock_code)
tradedate = datetime.datetime.strptime("2022-07-06", '%Y-%m-%d')
# df["ENTRYDATETIME"] = datetime.datetime.now()
df["ENTRYDATETIME"] = tradedate
print(df)
