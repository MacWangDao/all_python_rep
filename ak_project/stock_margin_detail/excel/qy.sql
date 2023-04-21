SELECT DISTINCT TO_CHAR(t1.QY_DATE, 'YYYY-MM-DD') "可用券单日期",
       t2.QUOTED_TIME "报价时间",
       t2.PROG_PROGRESS "方案进度",
       t1.DATA_SOURCE "券源券商",
       t1.STOCKCODE "股票代码",
       t1.SECURITY_NAME_ABBR "股票名称",
       t1.QY_TYPE "券单从属",
       t1.AVAILABILITY_PERIOD "可用周期长度",
       t1.VOL "可用量",
       t1.ESTIMATED_SIZE "估计可用量",
       t2.SUBSCRIBE_PRICE "认购门槛(万元)",
       t2.MAX_AMOUNT "金额上限(亿元)",
       t2.MAX_VOL "股数上限(万股)",
       t2.INSTORY_TYPE "申万行业分类",
       t2.PROVINCE "所属省份",
       t2.CITY "所属城市",
       t2.UNDERWRITER "承销商",
       t2.BALANCE_MARGIN_TRADING "融资融券余额(万元)"
FROM T_QY_BJHY_STOCK_MARGIN_DETAIL t1
         JOIN (
    SELECT t.QCT_TYPE,
           t.STOCKCODE,
           t.PROG_PROGRESS,
           t.QUOTED_TIME,
           t.SUBSCRIBE_PRICE,
           t.MAX_AMOUNT,
           t.MAX_VOL,
           t.INSTORY_TYPE,
           t.PROVINCE,
           t.CITY,
           t.UNDERWRITER,
           t.BALANCE_MARGIN_TRADING
    FROM T_QCT_BJHY_FUND_DETAIL t
    WHERE t.QCT_TYPE = '定增项目信息表' AND t.QUOTED_TIME = (
        SELECT CASE
                   WHEN trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') <= TO_DATE('20230413', 'yyyymmdd')
                       AND TO_DATE('20230413', 'yyyymmdd') < trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') + 10
                       THEN '预计' || to_char(TO_DATE('20230413', 'yyyymmdd'), 'fmmm') || '月上旬'
                   WHEN trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') + 10 <= TO_DATE('20230413', 'yyyymmdd')
                       AND TO_DATE('20230413', 'yyyymmdd') < trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') + 20
                       THEN '预计' || to_char(TO_DATE('20230413', 'yyyymmdd'), 'fmmm') || '月中旬'
                   WHEN TO_DATE('20230413', 'yyyymmdd') > trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') + 20
                       THEN '预计' || to_char(TO_DATE('20230413', 'yyyymmdd'), 'fmmm') || '月下旬'
                   ELSE NULL
                   END AS QUOTED_TIME
        FROM dual
    )
       OR t.QUOTED_TIME = (
        SELECT '预计' || decode(to_char(TO_DATE('20230413', 'yyyymmdd'), 'Q'), '1', '一', '2', '二', '3', '三', '4', '四') ||
               '季度' QUOTED_TIME
        from dual
    )
       OR t.QUOTED_TIME = (
        SELECT '预计' || to_char(TO_DATE('20230413', 'yyyymmdd'), 'fmmm') || '月份' q from dual
    )
       OR t.QUOTED_TIME = (
        SELECT CASE
                   WHEN trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') <= TO_DATE('20230413', 'yyyymmdd')
                       AND TO_DATE('20230413', 'yyyymmdd') < trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') + 10
                       THEN '预计' || to_char(TO_DATE('20230413', 'yyyymmdd'), 'fmmm') || '月初'
                   WHEN trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') + 10 <= TO_DATE('20230413', 'yyyymmdd')
                       AND TO_DATE('20230413', 'yyyymmdd') < trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') + 20
                       THEN '预计' || to_char(TO_DATE('20230413', 'yyyymmdd'), 'fmmm') || '月中'
                   WHEN TO_DATE('20230413', 'yyyymmdd') > trunc(TO_DATE('20230413', 'yyyymmdd'), 'mm') + 20
                       THEN '预计' || to_char(TO_DATE('20230413', 'yyyymmdd'), 'fmmm') || '月底'
                   ELSE NULL
                   END AS QUOTED_TIME
        FROM dual
    )
       OR t.QUOTED_TIME = '20230413'
       OR t.QUOTED_TIME = (
        SELECT '预计' || to_char(TO_DATE('20230413', 'yyyymmdd'), 'fmmm"月"dd"日"') q from dual
    )
) t2 ON t1.STOCKCODE = t2.STOCKCODE AND t1.QY_DATE = TO_DATE('20230413', 'yyyymmdd');