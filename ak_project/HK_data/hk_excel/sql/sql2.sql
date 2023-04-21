-- SELECT t.brokercode, t.brokername, ROUND(SUM(q.AVGPRICE * t.changenum)) AS sum_mul_chclose
-- FROM TQ_SK_BASICINFO s
--          JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode AND q.tradedate = 20230417
--          JOIN T_O_HK_BROKERHOLDNUM_NEW t ON t.stockcode = s.SYMBOL AND t.tradeday = to_date(20230417, 'yyyy/mm/dd')
-- GROUP BY t.brokercode, t.brokername
-- SELECT t.brokercode, t.brokername, ROUND(SUM(q.AVGPRICE * t.changenum)) AS sum_mul_chclose
-- FROM T_O_HK_BROKERHOLDNUM_NEW t,
--      TQ_QT_SKDAILYPRICE q,
--      TQ_SK_BASICINFO s
-- WHERE q.secode = s.secode
--   AND t.stockcode = s.SYMBOL
--   AND q.tradedate = 20230417
--   AND tradeday = to_date(20230417
--     , 'yyyy/mm/dd')
-- GROUP BY t.brokercode, t.brokername

SELECT t.stockcode,
       t.stockname,
       t.industryname,
       t.sum_changenum || ',' || q.CIRCSKAMT || ',' || q.CIRCSKAMT * 10000 AS text
FROM (SELECT stockcode,
             case substr(stockname, 1, 2)
                 when 'DR' then (select distinct(stockname)
                                 from T_O_HK_BROKERHOLDNUM_NEW f
                                 where f.stockname not like 'DR%'
                                   and f.stockcode = t.stockcode
                                   and rownum = 1)
                 when 'XD'
                     then (select distinct(stockname)
                           from T_O_HK_BROKERHOLDNUM_NEW f
                           where f.stockname not like 'XD%'
                             and f.stockcode = t.stockcode
                             and rownum = 1)
                 else stockname end stockname,
             industryname,
             SUM(changenum) AS      sum_changenum
      FROM T_O_HK_BROKERHOLDNUM_NEW t
      WHERE tradeday = to_date(20230403, 'yyyy/mm/dd')
      GROUP BY stockcode, stockname, industryname) t,
     TQ_SK_SHARESTRUCHG q,
     TQ_SK_BASICINFO s
WHERE q.compcode = s.compcode
  AND t.stockcode = s.SYMBOL
  and q.ENTRYDATE = (select max(g.ENTRYDATE)
                     from TQ_SK_SHARESTRUCHG g,
                          TQ_SK_BASICINFO l
                     where g.compcode = l.compcode
                       and l.symbol = s.symbol)
  and q.begindate = (select max(g.begindate)
                     from TQ_SK_SHARESTRUCHG g,
                          TQ_SK_BASICINFO l
                     where g.compcode = l.compcode
                       and l.symbol = s.symbol)
  and t.sum_changenum != 0