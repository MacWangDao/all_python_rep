import datetime

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

    # @logger.catch
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

    # @logger.catch
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

    # @logger.catch
    def __executemany(self, sql, args=None):
        """
        执行sql语句
        :param sql:     str     sql语句
        :param args:    list    sql语句参数列表
        :param return:  cursor
        """
        conn, cursor = self.__get_conn()

        if args:
            cursor.executemany(sql, args)
        else:
            cursor.executemany(sql)

        return conn, cursor

    # @logger.catch
    def fetch_all(self, sql, args=None):
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchall()
        # self.__reset_conn(conn, cursor)

        return result

    # @logger.catch
    def fetch_one(self, sql, args=None):
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchone()
        # self.__reset_conn(conn, cursor)

        return result

    # @logger.catch
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

    # @logger.catch
    def executemany_sql(self, sql, args=None):
        """
        执行SQL语句。
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__executemany(sql, args)
        conn.commit()
        self.__reset_conn(conn, cursor)

    def insert_many(self, table, columns, values_list):
        if columns:
            self.columns = ",".join(columns)
            self.valuse = ",".join([":" + str(i) for i in range(1, len(columns) + 1)])
        insert_sql = f"insert into {table} ({self.columns}) values ({self.valuse})"
        self.executemany_sql(insert_sql, args=values_list)

    def sql_select(self, table, find_columns, conditions):
        if find_columns:
            self.find_columns = ",".join(find_columns)
            self.valuse = ",".join([":" + str(i) for i in range(1, len(find_columns) + 1)])
        self.select_sql = f"SELECT  {self.find_columns} from {table} where 1=1 "
        if conditions:
            self.conditions = " and ".join([f"{its[0]}='{its[1]}'" for its in conditions])
            self.select_sql = self.select_sql + " and " + self.conditions
        res = self.fetch_all(self.select_sql)
        return res

    def __del__(self):
        """
        关闭连接池。
        """
        pass


@logger.catch
def get_trade_date_before(config):
    orcl = OracleConn(config)
    # sql = "SELECT TRADEDATE FROM fcdb.TQ_QT_SKDAILYPRICE ORDER BY TRADEDATE DESC"
    sql = "SELECT MAX(TRADEDATE)  FROM fcdb.TQ_QT_SKDAILYPRICE ORDER BY TRADEDATE DESC"
    trade_date = orcl.fetch_one(sql)
    sql = "select s.symbol,q.lclose from fcdb.TQ_QT_SKDAILYPRICE q, fcdb.TQ_SK_BASICINFO s where q.secode =  s.secode and q.tradedate =:1"
    result = orcl.fetch_all(sql, args=trade_date)
    if result:
        return dict(result)


def str_to_timestamp(date_str, format="%Y-%m-%d %H:%M:%S"):
    if date_str:
        timestamp = datetime.datetime.strptime(date_str, format).timestamp()
        return timestamp


def timestamp_to_str(timestamp, format="%Y-%m-%d %H:%M:%S"):
    if timestamp:
        return datetime.datetime.utcfromtimestamp(timestamp)


def local_time_timestamp(date_time=None):
    if date_time:
        return str_to_timestamp(date_time)
    now = datetime.datetime.now()
    otherStyleTime = now.strftime("%Y-%m-%d %H:%M:%S")
    return str_to_timestamp(otherStyleTime)


if __name__ == "__main__":
    # config = {
    #     'user': 'dazh',
    #     'password': 'dazh',
    #     'host': '192.168.101.215',
    #     'port': 1521,
    #     'service_name': 'dazh'
    # }
    config = {
        'user': 'bjhy',
        'password': 'bjhy',
        'host': '192.168.101.215',
        'port': 1521,
        'service_name': 'bjhy'
    }
    orcl = OracleConn(config)
    '''
    insert into t_o_csrc_official_news
  (csrc_id, original, title, summary, content, href, publisher_date, to_database_date, publisher_tp)
values
  (v_csrc_id, v_original, v_title, v_summary, v_content, v_href, v_publisher_date, v_to_database_date, v_publisher_tp);
    '''
    # sql = "insert into t_o_csrc_official_news (title,href) values (:1,:2)"
    # args = [("111", "www1"), ("2", "www2")]
    # orcl.executemany_sql(sql, args=args)
    table = "t_o_csrc_official_news"
    # values = [("111", "www1","www","2022-04-14")]
    columns = ["original", "title", "href", "publisher_date", "to_database_date"]
    # columns = ["original", "title", "href", "publisher_date"]
    values = [("证监会官网", "习近平在海南考察时强调：解放思想开拓创新团结奋斗攻坚克难 加快建设具有世界影响力的中国特色自由贸易港",
               "http://www.gov.cn/xinwen/2022-04/13/content_5685109.htm", "2022-04-14", datetime.datetime.now())]

    # orcl.insert_many(table, columns, values)
    # conditions = [("name", "xm"), ("age", 18)]
    # print(" and ".join([f"{its[0]}='{its[1]}'" for its in conditions]))
    # res = orcl.sql_select("T_O_RECORD_INFORMATION", ["UPDATE_LAST_CONTENT"],
    #                       [("NEWS_ORIGINAL", 1)])
    # print(res)
    # sql = "insert into T_O_RECORD_INFORMATION (NEWS_ORIGINAL,UPDATE_LAST_CONTENT,UPDATE_NEWS_DATETIME) values (:1,:2,:3)"
    # args = (1, "content", datetime.datetime.now())
    # orcl.execute_sql(sql, args=args)
    sql = "update T_O_RECORD_INFORMATION set update_last_content =:1,update_news_datetime =:2 where  news_original =:3"
    args = (b"qqqqqqqqq", datetime.datetime.now(), 1)
    orcl.execute_sql(sql, args=args)
