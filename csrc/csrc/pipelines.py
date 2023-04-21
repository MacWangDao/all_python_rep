# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import datetime

from csrc.oracleutils import OracleConn
import base64


class CsrcPipeline:
    item_list = []
    oracle_columns = ["ORIGINAL", "TITLE", "HREF", "PUBLISHER_DATE", "TO_DATABASE_DATE", "ORIGINAL_URL", "ORIGINAL_ID",
                      "REAL_DATE"]
    insert_data = {}
    update_data = {}
    repeate_data = {}

    def __init__(self, oracle_user, oracle_password, oracle_host, oracle_port, oracle_service_name, oracle_table):
        self.oracle_user = oracle_user
        self.oracle_password = oracle_password
        self.oracle_host = oracle_host
        self.oracle_port = oracle_port
        self.oracle_service_name = oracle_service_name
        self.oracle_table = oracle_table
        self.config = {
            'user': self.oracle_user,
            'password': self.oracle_password,
            'host': self.oracle_host,
            'port': self.oracle_port,
            'service_name': self.oracle_service_name
        }
        self.orcl = OracleConn(self.config)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(oracle_user=crawler.settings.get('ORACLE_USER'),
                   oracle_password=crawler.settings.get('ORACLE_PASSWORD'),
                   oracle_host=crawler.settings.get('ORACLE_HOST'),
                   oracle_port=crawler.settings.get('ORACLE_PORT'),
                   oracle_service_name=crawler.settings.get('ORACLE_SERVICE_NAME'),
                   oracle_table=crawler.settings.get('ORACLE_TABLE'))

    def open_spider(self, spider):
        res = self.orcl.fetch_all("SELECT NEWS_ORIGINAL, UPDATE_LAST_CONTENT from T_O_RECORD_INFORMATION")
        for k, v in res:
            self.repeate_data[k] = v.read().decode().split(";")
        # self.repeate_data = self.orcl.sql_select("T_O_RECORD_INFORMATION", ["UPDATE_LAST_CONTENT"],
        #                                          [("NEWS_ORIGINAL", 1)])
        # if self.repeate_data:
        #     for bob in self.repeate_data:
        #         self.repeate_data = bob[0].read().decode().split(";")

    def process_item(self, item, spider):
        # print(type(item))
        # page_data = (item["original"], item["title"])
        # self.item_list.append(tuple(page_data))
        # # self.orcl.insert_many(self.oracle_table)
        # print(self.item_list)

        if len(self.item_list) == 100:
            self.orcl.insert_many(self.oracle_table, self.oracle_columns, self.item_list)
            self.item_list = []
        else:
            if not self.repeate_data.get(item["update_id"]):
                # self.url_base64_list.append(base64.b64encode(item["href"].encode()).decode())
                if self.insert_data.get(item["update_id"]):
                    self.insert_data[item["update_id"]].append(base64.b64encode(item["href"].encode()).decode())
                else:
                    self.insert_data[item["update_id"]] = [base64.b64encode(item["href"].encode()).decode()]
                self.item_list.append(
                    (item["original"], item["title"], item["href"], item["publisher_date"], item["to_oracle_date"],
                     item["original_url"], item["original_id"], item["real_date"]))
            else:
                current_data = base64.b64encode(item["href"].encode()).decode()
                if current_data not in self.repeate_data.get(item["update_id"]):
                    # self.url_base64_list.append(base64.b64encode(item["href"].encode()).decode())
                    # self.update_data[item["update_id"]] = self.url_base64_list

                    # if self.update_data.get(item["update_id"]):
                    #     self.update_data[item["update_id"]].append(base64.b64encode(item["href"].encode()).decode())
                    # else:
                    #     self.update_data[item["update_id"]] = [base64.b64encode(item["href"].encode()).decode()]

                    self.item_list.append(
                        (item["original"], item["title"], item["href"], item["publisher_date"], item["to_oracle_date"],
                         item["original_url"], item["original_id"], item["real_date"]))
                    self.repeate_data[item["update_id"]].append(base64.b64encode(item["href"].encode()).decode())
                    self.update_data[item["update_id"]] = self.repeate_data[item["update_id"]]
                # else:
                #     if self.update_data.get(item["update_id"]):
                #         print(current_data)
                #         self.update_data[item["update_id"]].append(current_data)
        return item

    def close_spider(self, spider):
        if self.item_list:
            pass
            self.orcl.insert_many(self.oracle_table, self.oracle_columns, self.item_list)
        sql = "insert into T_O_RECORD_INFORMATION (NEWS_ORIGINAL,UPDATE_LAST_CONTENT,UPDATE_NEWS_DATETIME) values (:1,:2,:3)"
        insert_list = []
        if self.insert_data:
            for k, v in self.insert_data.items():
                insert_list.append(
                    (k, ";".join(v).encode(), datetime.datetime.now())
                )
            # content = ";".join(self.url_base64_list)
            # content = content.encode()
            # args = (1, content, datetime.datetime.now())
            if insert_list:
                self.orcl.executemany_sql(sql, args=insert_list)
        # content = ";".join(self.url_base64_list)
        # content = content.encode()
        # args = (content, datetime.datetime.now(), 1)
        update_list = []
        if self.update_data:
            for k, v in self.update_data.items():
                update_list.append(
                    (";".join(v).encode(), datetime.datetime.now(), k)
                )
            sql = "update T_O_RECORD_INFORMATION set update_last_content =:1,update_news_datetime =:2 where  news_original =:3"
            if update_list:
                self.orcl.executemany_sql(sql, args=update_list)

        # print(self.item_list)
        # self.insert_data(self.data_list)
        # self.conn.commit()
        # self.cur.close()
        # self.conn.close()


class MongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'), mongo_db=crawler.settings.get('MONGO_DB'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def process_item(self, item, spider):
        self.db[item.collection].insert(dict(item))
        return item

    def close_spider(self, spider):
        self.client.close()
