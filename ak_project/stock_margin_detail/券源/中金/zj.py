import pandas as pd

path = "/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/中金/T0券单 2023-02-08.xlsx"
df = pd.read_excel(path, sheet_name=0, dtype=str, skiprows=19)
df = df.iloc[:, 1:]
