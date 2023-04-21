import datetime
import re

import scrapy
from lxml import etree

from csrc.items import CsrcItem


class CsrcEnentSpider(scrapy.Spider):
    name = 'csrc_enent'
    allowed_domains = ['www.csrc.gov.cn']
    start_urls = ['http://www.csrc.gov.cn/']

    def start_requests(self):
        base_url = 'http://www.csrc.gov.cn/csrc/c100027/common_list.shtml'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse, encoding='utf-8')

    def parse(self, response):
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('//*[@id="list"]/li')
        total_count_list = selector.xpath('//*[@id="list"]/script[2]/text()')
        for total_count in total_count_list:
            find = re.findall(r".*?\((.*?)\).*?", total_count)
            for res in find:
                num = res.split(",")
                item["total_count"] = num[1]
        for li in li_list:
            item["original"] = "证监会官网"
            item["title"] = li.xpath('./a/text()')[0]
            item["href"] = li.xpath('./a/@href')[0]
            item["publisher_date"] = li.xpath('./span/text()')[0]
            item["to_oracle_date"] = datetime.datetime.now()
            yield item
