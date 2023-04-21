import cx_Oracle as Oracle
from loguru import logger


class OracleConn:
    def __init__(self, config):
        self.__config = config
        self.__dns = self.__get_dsn(config)

    @staticmethod
    def __get_dsn(config):
        dsn = None
        host, port = config.get('host'), config.get('port')

        if 'service_name' in config:
            dsn = Oracle.makedsn(host, port, service_name=config.get('service_name'))

        elif 'sid' in config:
            dsn = Oracle.makedsn(host, port, sid=config.get('sid'))

        return dsn

    @logger.catch
    def __get_conn(self):
        """
        从连接池中获取一个连接，并获取游标。
        :return: conn, cursor
        """
        conn = Oracle.connect(self.__config.get('user'), self.__config.get('password'), self.__dns)
        cursor = conn.cursor()
        self.__conn = conn

        return conn, cursor

    @staticmethod
    def __reset_conn(conn, cursor):
        """
        把连接放回连接池。
        :return:
        """
        cursor.close()
        conn.close()

    @logger.catch
    def __execute(self, sql, args=None):
        """
        执行sql语句
        :param sql:     str     sql语句
        :param args:    list    sql语句参数列表
        :param return:  cursor
        """
        conn, cursor = self.__get_conn()

        if args:
            cursor.execute(sql, args)
        else:
            cursor.execute(sql)

        return conn, cursor

    @logger.catch
    def fetch_all(self, sql, args=None):
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchall()
        self.__reset_conn(conn, cursor)

        return result

    @logger.catch
    def fetch_one(self, sql, args=None):
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchone()
        self.__reset_conn(conn, cursor)

        return result

    @logger.catch
    def execute_sql(self, sql, args=None):
        """
        执行SQL语句。
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        conn.commit()
        self.__reset_conn(conn, cursor)

    def __del__(self):
        """
        关闭连接池。
        """
        pass


@logger.catch
def get_lclose_price(config, trade_date=None, instrument=None):
    orcl = OracleConn(config)
    # sql = "SELECT TRADEDATE FROM fcdb.TQ_QT_SKDAILYPRICE ORDER BY TRADEDATE DESC"
    sql = "SELECT MAX(TRADEDATE)  FROM fcdb.TQ_QT_SKDAILYPRICE ORDER BY TRADEDATE DESC"
    trade_date = orcl.fetch_one(sql) if trade_date is None else trade_date
    sql = "select s.symbol,q.lclose from fcdb.TQ_QT_SKDAILYPRICE q, fcdb.TQ_SK_BASICINFO s where q.secode =  s.secode and q.tradedate =:1 "
    if instrument:
        sql += "and s.symbol = '%s'" % (instrument)
    result = orcl.fetch_all(sql, args=trade_date)
    if result:
        return dict(result)


if __name__ == "__main__":
    config = {
        'user': 'dazh',
        'password': 'dazh',
        'host': '192.168.101.215',
        'port': 1521,
        'service_name': 'dazh'
    }
    result = get_trade_date_before(config)
    print(result)
