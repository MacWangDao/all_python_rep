from abc import ABC

import tornado.web
import tornado.ioloop
import tornado.httpserver
import tornado.options
from tornado.options import options, define
from tornado.web import RequestHandler
import json
from csrc.oracleutils import OracleConn
import pandas as pd

define("port", default=8080, type=int, help="run server on the given port.")
config = {
    'user': 'fcdb',
    'password': 'fcdb',
    'host': '192.168.101.215',
    'port': 1521,
    'service_name': 'dazh'
}
orcl = OracleConn(config)


class IndexHandler(RequestHandler):
    def get(self):
        offsetId = self.get_query_argument('offsetId', 0)
        count = self.get_query_argument('count', 0)
        data = {"data": []}
        sql = "SELECT CSRC_ID,ORIGINAL,TITLE,HREF,REAL_DATE,ORIGINAL_URL from T_O_CSRC_OFFICIAL_NEWS where LENGTH(REAL_DATE) = 10 AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'dd')=to_char(sysdate,'dd') AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'mm')=to_char(sysdate,'mm')"
        result = orcl.fetch_all(sql)
        for res in result:
            data["data"].append({
                "CSRC_ID": res[0],
                "ORIGINAL": res[1],
                "TITLE": res[2],
                "HREF": res[3],
                "PUBLISHER_DATE": res[4],
                "ORIGINAL_URL": res[5],
            })
        res_data = json.dumps(data)
        self.write(res_data)

    def post(self):
        arg = self.get_body_argument('offsetId')


class GetNewsListHandler(RequestHandler, ABC):
    def get(self):
        offsetId = self.get_query_argument('offsetId', 0)
        count = self.get_query_argument('count', 0)
        data = {"data": []}
        # sql = "SELECT CSRC_ID,ORIGINAL,TITLE,HREF,PUBLISHER_DATE,ORIGINAL_URL FROM T_O_CSRC_OFFICIAL_NEWS where rownum <= 200"
        sql = "SELECT CSRC_ID,ORIGINAL,TITLE,HREF,REAL_DATE,ORIGINAL_URL from T_O_CSRC_OFFICIAL_NEWS where LENGTH(REAL_DATE) = 10 AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'dd')=to_char(sysdate,'dd') AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'mm')=to_char(sysdate,'mm')"
        if offsetId != 0:
            sql = f"SELECT CSRC_ID,ORIGINAL,TITLE,HREF,REAL_DATE,ORIGINAL_URL from T_O_CSRC_OFFICIAL_NEWS where CSRC_ID > {offsetId} AND LENGTH(REAL_DATE) = 10 AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'dd')=to_char(sysdate,'dd') AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'mm')=to_char(sysdate,'mm')"
        result = orcl.fetch_all(sql)
        for res in result:
            data["data"].append({
                "CSRC_ID": res[0],
                "ORIGINAL": res[1],
                "TITLE": res[2],
                "HREF": res[3],
                "PUBLISHER_DATE": res[4],
                "ORIGINAL_URL": res[5],
            })
        res_data = json.dumps(data)
        self.write(res_data)

class GetNewsListHandler_drop_duplicates(RequestHandler, ABC):
    def get(self):
        offsetId = self.get_query_argument('offsetId', 0)
        count = self.get_query_argument('count', 0)
        data = {"data": []}
        # sql = "SELECT CSRC_ID,ORIGINAL,TITLE,HREF,PUBLISHER_DATE,ORIGINAL_URL FROM T_O_CSRC_OFFICIAL_NEWS where rownum <= 200"
        sql = "SELECT CSRC_ID,ORIGINAL,TITLE,HREF,REAL_DATE,ORIGINAL_URL from T_O_CSRC_OFFICIAL_NEWS where LENGTH(REAL_DATE) = 10 AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'dd')=to_char(sysdate,'dd') AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'mm')=to_char(sysdate,'mm')"
        if offsetId != 0:
            sql = f"SELECT CSRC_ID,ORIGINAL,TITLE,HREF,REAL_DATE,ORIGINAL_URL from T_O_CSRC_OFFICIAL_NEWS where CSRC_ID > {offsetId} AND LENGTH(REAL_DATE) = 10 AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'dd')=to_char(sysdate,'dd') AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'mm')=to_char(sysdate,'mm')"
        result = orcl.fetch_all(sql)
        res_list = []
        for res in result:
            res_list.append({
                "CSRC_ID": res[0],
                "ORIGINAL": res[1],
                "TITLE": res[2],
                "HREF": res[3],
                "PUBLISHER_DATE": res[4],
                "ORIGINAL_URL": res[5],
            })
        df = pd.DataFrame(res_list)
        df.drop_duplicates(subset="TITLE", inplace=True, keep="first")
        res = df.to_dict(orient="records")
        # res = df.to_json(orient="records")
        # res_data = json.dumps({})
        data["data"] = res
        res_data = json.dumps(data)
        self.write(res_data)

class NewsListHandler(RequestHandler, ABC):
    def get(self, subject, city):
        # self.write(("Subject: %s<br/>City: %s" % (subject, city)))
        data = {"data": []}
        # sql = "SELECT CSRC_ID,ORIGINAL,TITLE,HREF,PUBLISHER_DATE,ORIGINAL_URL FROM T_O_CSRC_OFFICIAL_NEWS where rownum <= 200"
        sql = "SELECT CSRC_ID,ORIGINAL,TITLE,HREF,REAL_DATE,ORIGINAL_URL from T_O_CSRC_OFFICIAL_NEWS where LENGTH(REAL_DATE) = 10 AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'dd')=to_char(sysdate,'dd') AND to_char(to_date(REAL_DATE,'yyyy-mm-dd'),'mm')=to_char(sysdate,'mm')"
        result = orcl.fetch_all(sql)
        for res in result:
            data["data"].append({
                "CSRC_ID": res[0],
                "ORIGINAL": res[1],
                "TITLE": res[2],
                "HREF": res[3],
                "PUBLISHER_DATE": res[4],
                "ORIGINAL_URL": res[5],
            })
        res_data = json.dumps(data)
        self.write(res_data)


class SubjectDateHandler(RequestHandler):
    def get(self, date, subject):
        self.write(("Date: %s<br/>Subject: %s" % (date, subject)))


if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application([
        (r"/", IndexHandler),
        (r"/getNews", GetNewsListHandler_drop_duplicates),
        (r"/test", GetNewsListHandler_drop_duplicates),
        (r"/getNews/", GetNewsListHandler),
        (r"/getNews/(.+)/([a-z]+)", NewsListHandler),  # 无名方式
        (r"/sub-date/(?P<subject>.+)/(?P<date>\d+)", SubjectDateHandler),  # 命名方式
    ])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()
