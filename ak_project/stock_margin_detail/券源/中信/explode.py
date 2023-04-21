import pandas as pd

path = "/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/中信/T+1券源模板 2023-02-01.xlsx"
df = pd.read_excel(path, sheet_name=0, dtype={1: str})
df = df.iloc[:, 1:6]
print(df.columns)
# print(df)

df['可提供期限'] = df['可提供期限'].map(lambda x: x.split('/'))
print(df)
result = df.explode('可提供期限')
print(result)
