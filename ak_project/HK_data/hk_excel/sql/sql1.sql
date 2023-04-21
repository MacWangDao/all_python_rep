SELECT *
FROM (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_60
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND SUBSTR(stockcode, 1, 2) = '60'
                    AND changenum > 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}'
     ),
     (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_00
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND SUBSTR(stockcode, 1, 2) = '00'
                    AND changenum > 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}'
     ),
     (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_30
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND SUBSTR(stockcode, 1, 2) = '30'
                    AND changenum > 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}'
     ),
     (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_688
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND SUBSTR(stockcode, 1, 3) = '688'
                    AND changenum > 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}'
     ),
     (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND changenum > 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}')
UNION All
SELECT *
FROM (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_60
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND SUBSTR(stockcode, 1, 2) = '60'
                    AND changenum < 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}'
     ),
     (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_00
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND SUBSTR(stockcode, 1, 2) = '00'
                    AND changenum < 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}'
     ),
     (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_30
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND SUBSTR(stockcode, 1, 2) = '30'
                    AND changenum < 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}'
     ),
     (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose_688
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND SUBSTR(stockcode, 1, 3) = '688'
                    AND changenum < 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}'
     ),
     (
         SELECT ROUND(SUM(t.sum_szb * q.tclose) / 100000000) AS sum_mul_tclose
         FROM (
                  SELECT stockcode, stockname, SUM(changenum) AS sum_szb
                  FROM T_O_HK_BROKERHOLDNUM_NEW
                  WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
                    AND changenum < 0
                  GROUP BY stockcode, stockname
              ) t
                  JOIN TQ_SK_BASICINFO s ON t.stockcode = s.SYMBOL
                  JOIN TQ_QT_SKDAILYPRICE q ON s.secode = q.secode
         WHERE q.tradedate = '{date}')
UNION ALL
SELECT *
FROM (select count(1) AS hzb
      FROM T_O_HK_BROKERHOLDNUM_NEW
      WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
        AND SUBSTR(stockcode, 1, 2) = '60') c1,
     (select count(1) AS szb
      FROM T_O_HK_BROKERHOLDNUM_NEW
      WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
        AND SUBSTR(stockcode, 1, 2) = '00') s2,
     (select count(1) AS scy
      FROM T_O_HK_BROKERHOLDNUM_NEW
      WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
        AND SUBSTR(stockcode, 1, 2) = '30') c3,
     (select count(1) AS hkc
      FROM T_O_HK_BROKERHOLDNUM_NEW
      WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
        AND SUBSTR(stockcode, 1, 3) = '688') c4,
     (select count(1) AS alsh FROM T_O_HK_BROKERHOLDNUM_NEW WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) c5
UNION ALL
SELECT *
FROM (select ROUND(sum(marketvalue) / 100000000, 0) AS hzb
      FROM T_O_HK_BROKERHOLDNUM_NEW
      WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
        AND SUBSTR(stockcode, 1, 2) = '60') c1,
     (select ROUND(sum(marketvalue) / 100000000, 0) AS szb
      FROM T_O_HK_BROKERHOLDNUM_NEW
      WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
        AND SUBSTR(stockcode, 1, 2) = '00') s2,
     (select ROUND(sum(marketvalue) / 100000000, 0) AS scy
      FROM T_O_HK_BROKERHOLDNUM_NEW
      WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
        AND SUBSTR(stockcode, 1, 2) = '30') c3,
     (select ROUND(sum(marketvalue) / 100000000, 0) AS hkc
      FROM T_O_HK_BROKERHOLDNUM_NEW
      WHERE tradeday = to_date({date}, 'yyyy/mm/dd')
        AND SUBSTR(stockcode, 1, 3) = '688') c4,
     (select ROUND(sum(marketvalue) / 100000000, 0) AS alsh
      FROM T_O_HK_BROKERHOLDNUM_NEW
      WHERE tradeday = to_date({date}, 'yyyy/mm/dd')) c5;