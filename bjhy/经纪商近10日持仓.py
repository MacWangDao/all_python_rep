def history_stock_MARKETVALUE(tradedate_list):
    """
    经纪商近10日持仓持仓市值(万)
    """
    try:
        tradedate_list.sort(reverse=True)
 
        sql = f"""select *  from (select to_char(t.tradeday,'yyyymmdd' ) tradeday,  t.brokername,  round(sum(t.MARKETVALUE) / 10000) changenum  from T_O_BrokerAllPosition t  where t.tradeday >= to_date({tradedate_list [ -1 ] }, 'yyyy/mm/dd')  and t.changenum != 0  group by t.tradeday,  t.brokername ) PIVOT(max(changenum)  FOR tradeday IN {tradedate_list })"""
        df_changenum_list = pd.read_sql(sql, engine, chunksize=50000)
        dflist = []
        for chunk in df_changenum_list:
            dflist.append(chunk)
        df_changenum = pd.concat(dflist)
        df_changenumcolumns = ["brokername",]
        label = df_changenumcolumns + [i[-4:] for i in tradedate_list]
        # df = pd.DataFrame(df_changenum , columns=label)
        df = pd.DataFrame(df_changenum )
        df.columns = label
        df.fillna(0, inplace=True)
        df.set_index(["brokername"], drop=True, append=False, inplace=True)
        print(df)

        
        return df
    except:
        return pd.DataFrame()
        
