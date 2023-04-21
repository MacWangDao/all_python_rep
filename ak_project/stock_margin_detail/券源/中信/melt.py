import pandas as pd

"""
name  数学  语文
0   张三  86  34
1   李四  56  78
2   王五  87  95
------------------------
转换后: 
  name course  score
0   张三     数学     86
1   李四     数学     56
2   王五     数学     87
3   张三     语文     34
4   李四     语文     78
5   王五     语文     95


"""
path = "/home/toptrade/ECommerceCrawlers/ak_project/stock_margin_detail/券源/中信/中信券单列表20230201091117.xlsx"
df = pd.read_excel(path, sheet_name=0, dtype={0: str})
df['证券代码'] = df['证券代码'].str.zfill(6)
print(df.columns)
df.columns = ["证券代码", "证券名称", "7", "14", "28", "91", "182", "备注"]
test_data = pd.melt(frame=df  # 待转换df
                    , id_vars=['证券代码', '证券名称']  # 固定的列
                    , value_vars=['7', '14', '28', '91', '182']  # 待转换的列名
                    , var_name='期限'  # 行转列转换后的列名称
                    , value_name='数量'  # 最后数据列名称
                    )
# test_data['证券代码'] = test_data['证券代码'].str.zfill(6)
# print(test_data)
print("1213".zfill(6))
print(test_data['证券代码'])
df = df.iloc[:, [0, -1]]
df = pd.merge(test_data, df, how='inner', on="证券代码")
# df = pd.merge(test_data, df)

print(df.columns)
s = "中信券单列表20230201091117.xlsx".split("中信券单列表")
print(s[1].split(".")[0][:8])