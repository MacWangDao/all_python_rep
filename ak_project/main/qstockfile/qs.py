import qstock as qs

# df = qs.realtime_data()
# 查看前几行

# print(df)
# df = qs.realtime_data('行业板块')
# print(df)
df = qs.stock_snapshot("300059")
print(df)
