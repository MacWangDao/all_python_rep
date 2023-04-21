import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth', 100)
path = "/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/广发/专项券源名单 2023-02-02.xlsx"
df = pd.read_excel(path, sheet_name=0, dtype={1: str, 3: str})
df = df.iloc[:, 1:6]
print(df.columns)
# print(df)
df["期限"] = df["期限"].str.extract("(\d+/.*\d|\d+)", expand=False)
print(df["期限"])

df['期限'] = df['期限'].map(lambda x: x.split('/'))
result = df.explode('期限')
# print(result)
