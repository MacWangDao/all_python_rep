import scrapy
from lxml import etree
import re
import datetime
import json

from csrc.items import CsrcItem

from csrc.oracleutils import timestamp_to_str


class CsrcNewsSpider(scrapy.Spider):
    name = 'csrc_news'
    allowed_domains = ['www.csrc.gov.cn']
    start_urls = ['http://www.csrc.gov.cn/']

    def start_requests(self):
        base_url = "http://www.csrc.gov.cn/searchList/a1a078ee0bc54721ab6b148884c784a8?_isAgg=true&_isJson=true&_pageSize=18&_template=index&_rangeTimeGte=&_channelName=&page=1"
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse, encoding='utf-8')

    def parse(self, response):
        item = CsrcItem()
        for res in json.loads(response.body).get("data", {}).get("results", []):
            item["original"] = res.get("channelName")
            item["title"] = res.get("title")
            item["href"] = res.get("url")
            item["publisher_date"] = res.get("publishedTimeStr")
            item["content"] = res.get("content")
            item["to_oracle_date"] = datetime.datetime.now()
            # print(timestamp_to_str(res.get("publishedTime")))

            yield item
