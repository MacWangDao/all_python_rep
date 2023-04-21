import datetime
import json
import re
import traceback

import scrapy
from lxml import etree

from csrc.items import CsrcItem
from selenium import webdriver
import platform
import sys
import os

current_path = os.path.dirname(os.path.abspath(__file__))
father_path = os.path.dirname(current_path)
father_path = os.path.dirname(father_path)


class CsrcNewsInfoSpider(scrapy.Spider):
    name = "csrc_news_info"
    allowed_domains = ['www.csrc.gov.cn']
    start_urls = ['http://www.csrc.gov.cn/']

    def __init__(self):
        options = webdriver.ChromeOptions()
        # options = Options()  # 实例化Option对象
        options.add_argument('--headless')  # 设置无界面
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        """
        sudo mount -t tmpfs -o rw,nosuid,nodev,noexec,relatime,size=512M tmpfs /dev/shm
        """
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2,
                                                  "profile.managed_default_content_settings.flash": 1})
        if platform.system().lower() == "linux":
            self.webdriver = webdriver.Chrome(
                executable_path=os.path.join(father_path, "chromedriver_linux64/chromedriver"),
                options=options)
        else:
            self.webdriver = webdriver.Chrome(
                executable_path=os.path.join(father_path, "chromedriver_win32/chromedriver.exe"),
                options=options)
        self.webdriver.implicitly_wait(10)  # 等待元素最多10s
        self.webdriver.set_page_load_timeout(10)  # 页面10秒后强制中断加载

    def closed(self, spider):
        self.webdriver.quit()

    def start_requests(self):
        base_url = 'http://www.csrc.gov.cn/csrc/c100027/common_list.shtml'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_csrc_newboard, encoding='utf-8',
                             meta={"_id": 1})
        base_url = "http://www.csrc.gov.cn/searchList/a1a078ee0bc54721ab6b148884c784a8?_isAgg=true&_isJson=true&_pageSize=18&_template=index&_rangeTimeGte=&_channelName=&page=1"
        yield scrapy.Request(url=base_url, method='GET', meta={"method": "json", "_id": 2},
                             callback=self.parse_csrc_event,
                             encoding='utf-8')
        base_url = 'http://www.gov.cn/guowuyuan/index.htm'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_gov_newboard, encoding='utf-8',
                             meta={"_id": 3})
        base_url = 'http://www.cbirc.gov.cn/cn/static/data/DocInfo/SelectItemAndDocByItemPId/data_itemId=914,pageSize=10.json'
        yield scrapy.Request(url=base_url, method='GET', meta={"method": "json", "_id": 4},
                             callback=self.parse_cbirc_newboard,
                             encoding='utf-8')
        base_url = 'http://www.pbc.gov.cn/goutongjiaoliu/113456/113469/index.html'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_pboc_newboard, encoding='utf-8',
                             meta={"_id": 5})
        base_url = 'http://www.mof.gov.cn/zhengwuxinxi/caizhengxinwen/'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_mof_newboard, encoding='utf-8',
                             meta={"_id": 6})
        base_url = 'http://www.mof.gov.cn/zhengwuxinxi/zhengcefabu/'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_mof_newboard, encoding='utf-8',
                             meta={"_id": 7})
        base_url = 'https://www.ndrc.gov.cn/xwdt/xwfb/?code=&state=123'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_ndrc_xwfb_newboard, encoding='utf-8',
                             meta={"_id": 8})
        base_url = 'https://www.ndrc.gov.cn/xwdt/szyw/?code=&state=123'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_ndrc_xwfb_newboard, encoding='utf-8',
                             meta={"_id": 9})
        base_url = 'http://www.stats.gov.cn/tjsj/zxfb/'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_stats_newboard, encoding='utf-8',
                             meta={"_id": 10})
        base_url = 'http://www.sse.com.cn/home/component/news/'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_sse_newboard, encoding='utf-8',
                             meta={"_id": 11})
        base_url = 'http://www.sse.com.cn/aboutus/mediacenter/hotandd/'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_sse_newboard, encoding='utf-8',
                             meta={"_id": 12})
        base_url = 'http://www.szse.cn/index/news/index.html'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_szse_ss_newboard, encoding='utf-8',
                             meta={"_id": 13})
        base_url = 'http://www.szse.cn/aboutus/trends/news/'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_szse_bs_newboard, encoding='utf-8',
                             meta={"_id": 14})
        # base_url = 'https://www.stcn.com/xw/'
        # yield scrapy.Request(url=base_url, method='GET', callback=self.parse_stcn_xw_newboard, encoding='utf-8',
        #                      meta={"_id": 15})
        # base_url = 'https://www.stcn.com/xw/'
        # yield scrapy.Request(url=base_url, method='GET', callback=self.parse_stcn_xw_newboard, encoding='utf-8',
        #                      meta={"_id": 15})
        # base_url = 'http://www.stcn.com/kuaixun/'
        # yield scrapy.Request(url=base_url, method='GET', callback=self.parse_stcn_kx_newboard, encoding='utf-8',
        #                      meta={"_id": 16})
        base_url = 'https://www.stcn.com/article/list/kx.html'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_stcn_kx_newboard_v2, encoding='utf-8',
                             meta={"_id": 16})
        base_url = 'http://www.zqrb.cn/finance/hongguanjingji/index.html'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_zqrb_finance_newboard, encoding='utf-8',
                             meta={"_id": 17})
        base_url = 'http://www.zqrb.cn/stock/gupiaoyaowen/index.html'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_zqrb_stock_newboard, encoding='utf-8',
                             meta={"_id": 18})
        base_url = 'http://www.zqrb.cn/stock/redian/index.html'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_zqrb_stock_red_newboard, encoding='utf-8',
                             meta={"_id": 19})
        base_url = 'https://www.cs.com.cn/xwzx/hg/'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_cs_hg_newboard, encoding='utf-8',
                             meta={"_id": 20})
        base_url = 'https://news.cnstock.com/news/sns_yw/index.html'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_cnstock_news_yw_newboard, encoding='utf-8',
                             meta={"_id": 21})
        base_url = 'https://news.cnstock.com/news/sns_jg/index.html'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_cnstock_news_jg_newboard, encoding='utf-8',
                             meta={"_id": 22})
        base_url = 'http://www.news.cn/politicspro/'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_news_politicspro_newboard,
                             encoding='utf-8', meta={"_id": 23})
        # base_url = 'http://www.news.cn/fortunepro/'
        # yield scrapy.Request(url=base_url, method='GET', callback=self.parse_news_fortunepro_newboard,
        #                      encoding='utf-8', meta={"_id": 24})
        base_url = 'http://da.wa.news.cn/nodeart/page?nid=115033'
        yield scrapy.Request(url=base_url, method='GET', callback=self.parse_news_fortunepro_newboard_v2,
                             encoding='utf-8', meta={"_id": 24})

    def parse_csrc_event(self, response):

        '''
        证监会要闻
        '''
        item = CsrcItem()
        for res in json.loads(response.body).get("data", {}).get("results", []):
            try:
                item["original_id"] = 1
                item["update_id"] = response.meta['_id']
                item["original_url"] = "http://www.csrc.gov.cn/csrc/c100028/common_xq_list.shtml"
                item["original"] = res.get("channelName")
                item["title"] = res.get("title")
                item["href"] = res.get("url")
                item["publisher_date"] = res.get("publishedTimeStr")
                item["real_date"] = datetime.datetime.strptime(item["publisher_date"], "%Y-%m-%d %H:%M:%S").strftime(
                    "%Y-%m-%d")
                item["content"] = res.get("content")
                item["to_oracle_date"] = datetime.datetime.now()
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_csrc_newboard(self, response):
        '''
        证监会官网
        '''
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
            try:
                item["original"] = "证监会官网"
                item["original_id"] = 1
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = li.xpath('./a/text()')[0]
                item["href"] = li.xpath('./a/@href')[0]
                item["publisher_date"] = li.xpath('./span/text()')[0]
                item["to_oracle_date"] = datetime.datetime.now()
                item["real_date"] = item["publisher_date"]
                # item["publisher_date"] = datetime.datetime.strptime(publisher_date[0], "%Y-%m/%d").strftime("%Y-%m-%d")
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_gov_newboard(self, response):
        '''
        国务院
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div[1]/div[1]/div/div[2]/div[3]/div[2]/div/div/ul/li')
        for li in li_list:
            try:
                item["original"] = "国务院"
                item["original_id"] = 2
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = li.xpath('./h4/a/text()')[0]
                item["href"] = li.xpath('./h4/a/@href')[0]
                if item["href"].find("http://") != 0:
                    base_url = f'http://www.gov.cn{item["href"]}'
                    item["href"] = base_url
                item["to_oracle_date"] = datetime.datetime.now()
                publisher_date = re.findall(r"http://www.gov.cn/.*?/(.*)/.*", item["href"])
                if not publisher_date:
                    publisher_date = re.findall(r"/.*?/(.*)/.*", item["href"])
                if publisher_date:
                    item["publisher_date"] = datetime.datetime.strptime(publisher_date[0], "%Y-%m/%d").strftime(
                        "%Y-%m-%d")
                    item["real_date"] = datetime.datetime.strptime(publisher_date[0], "%Y-%m/%d").strftime(
                        "%Y-%m-%d")
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_cbirc_newboard(self, response):
        '''
        中国银行保险监督管理委员会
        '''
        item = CsrcItem()
        for res in json.loads(response.body).get("data", []):
            itemId = res.get("itemId", "")
            if itemId == 915 and res.get("itemName", "") == "监管动态":
                docInfoVOList = res.get("docInfoVOList", [])
                for docinfo in docInfoVOList:
                    try:
                        docId = docinfo.get("docId", "")
                        docTitle = docinfo.get("docTitle", "")
                        publishDate = docinfo.get("publishDate", "")
                        docFileUrl = docinfo.get("docFileUrl", "")
                        # isTitleLink = docinfo.get("isTitleLink", "")
                        item["original"] = "中国银行保险监督管理委员会"
                        item["original_id"] = 3
                        item["update_id"] = response.meta['_id']
                        item[
                            "original_url"] = "http://www.cbirc.gov.cn/cn/view/pages/ItemList.html?itemPId=914&itemId=915&itemUrl=ItemListRightList.html&itemName=%E7%9B%91%E7%AE%A1%E5%8A%A8%E6%80%81"
                        item["title"] = docTitle
                        item[
                            "href"] = f"https://www.cbirc.gov.cn/cn/view/pages/ItemDetail.html?docId={docId}&itemId={itemId}&generaltype=0"
                        item["publisher_date"] = publishDate
                        item["real_date"] = datetime.datetime.strptime(item["publisher_date"],
                                                                       "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
                        item["content"] = docFileUrl
                        item["to_oracle_date"] = datetime.datetime.now()
                        yield item
                    except Exception as e:
                        traceback.print_exc(file=open('error.txt', 'a'))
            else:
                continue

    def parse_pboc_newboard(self, response):
        '''
        中国人民银行
        '''
        '''
        //*[@id="11040"]/div[2]/div[1]/table/tbody/tr[2]/td/table[1]/tbody/tr/td[2]
        /html/body/div[4]/table[2]/tbody/tr/td[3]/table/tbody/tr/td/div/div/div[2]/div[1]/table/tbody/tr[2]/td
        //*[@id="11040"]/div[2]/div[1]/table/tbody/tr[2]/td/table[1]
        //*[@id="11040"]/div[2]/div[1]/table/tbody/tr[2]/td
        //*[@id="11040"]/div[2]/div[1]/table/tbody/tr[2]/td/table[1]
        //*[@id="11040"]/div[2]/div[1]/table/tbody/tr[2]/td/table[1]/tbody/tr/td[2]/font/a
        //*[@id="11040"]/div[2]/div[1]/table/tbody/tr[2]/td/table[1]/tbody/tr/td[2]/span
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        table_list = selector.xpath('//*[@id="11040"]/div[2]/div[1]/table/tbody/tr[2]/td/table')
        for table in table_list:
            try:
                item["original"] = "中国人民银行"
                item["original_id"] = 4
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = table.xpath('./tbody/tr/td[2]/font/a/text()')[0]
                item["href"] = "http://www.pbc.gov.cn" + table.xpath('./tbody/tr/td[2]/font/a/@href')[0]
                item["publisher_date"] = table.xpath('./tbody/tr/td[2]/span/text()')[0]
                item["to_oracle_date"] = datetime.datetime.now()
                item["real_date"] = item["publisher_date"]
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_mof_newboard(self, response):
        '''
        中华人民共和国财政部
        '''
        '''
        /html/body/div[4]/div[1]/ul[1]
        /html/body/div[4]/div[1]/ul[1]/li[1]/a
        /html/body/div[4]/div[1]/ul[1]/li[1]/span
        /html/body/div[4]/div[1]/ul[1]/li[1]/a
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        ul_list = selector.xpath('/html/body/div[4]/div[1]/ul')
        for ul in ul_list:
            for li in ul.xpath('./li'):
                try:
                    item["original"] = "中华人民共和国财政部"
                    item["original_id"] = 5
                    item["update_id"] = response.meta['_id']
                    item["original_url"] = response.url
                    item["title"] = li.xpath('./a/text()')[0]
                    item["href"] = li.xpath('./a/@href')[0]
                    if item["href"].find("./") == 0:
                        base_url = f'http://www.mof.gov.cn/zhengwuxinxi/caizhengxinwen{item["href"][1:]}'
                        item["href"] = base_url
                    elif item["href"].find("../") == 0:
                        base_url = f'http://www.mof.gov.cn/zhengwuxinxi{item["href"][2:]}'
                        item["href"] = base_url
                    item["publisher_date"] = li.xpath('./span/text()')[0]
                    item["to_oracle_date"] = datetime.datetime.now()
                    item["real_date"] = item["publisher_date"]
                    yield item
                except Exception as e:
                    traceback.print_exc(file=open('error.txt', 'a'))

    def parse_ndrc_xwfb_newboard(self, response):
        '''
        中华人民共和国国家发展和改革委员会
        '''
        '''
        /html/body/div[2]/div[2]/ul/li
        https://www.ndrc.gov.cn/xwdt/xwfb/202204/t20220401_1321525.html?code=&state=123
        /html/body/div[2]/div[2]/ul/li[1]/a
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div[2]/div[2]/ul/li')
        for li in li_list:
            if li.xpath('./a/text()'):
                try:
                    item["original"] = "中华人民共和国国家发展和改革委员会"
                    item["original_id"] = 6
                    item["update_id"] = response.meta['_id']
                    item["original_url"] = response.url
                    item["title"] = li.xpath('./a/text()')[0]
                    item["href"] = li.xpath('./a/@href')[0]
                    if item["href"].find("./") == 0:
                        base_url = f'https://www.ndrc.gov.cn/xwdt/xwfb{item["href"][1:]}?code=&state=123'
                        item["href"] = base_url
                    item["publisher_date"] = li.xpath('./span/text()')[0]
                    item["to_oracle_date"] = datetime.datetime.now()
                    item["real_date"] = datetime.datetime.strptime(item["publisher_date"],
                                                                   "%Y/%m/%d").strftime("%Y-%m-%d")
                    yield item
                except Exception as e:
                    traceback.print_exc(file=open('error.txt', 'a'))

    def parse_stats_newboard(self, response):
        '''
        国家统计局
        '''
        '''
        http://www.stats.gov.cn/tjsj/zxfb/
        http://www.stats.gov.cn/tjsj/sjjd/202204/t20220415_1829622.html
        http://www.stats.gov.cn/tjsj/zxfb/202204/t20220411_1829486.html
        /html/body/div/div/div[3]/div[2]/ul/li[1]/a/span/font[1]
        /html/body/div/div/div[3]/div[2]/ul/li[1]/a/span/font[2]
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div/div/div[3]/div[2]/ul/li')
        for li in li_list:
            if li.xpath('./a'):
                try:
                    item["original"] = "国家统计局"
                    item["original_id"] = 7
                    item["update_id"] = response.meta['_id']
                    item["original_url"] = response.url
                    item["title"] = li.xpath('./a/span/font[1]/text()')[0]
                    item["href"] = li.xpath('./a/@href')[0]
                    if item["href"].find("./") == 0:
                        base_url = f'http://www.stats.gov.cn/tjsj/zxfb{item["href"][1:]}'
                        item["href"] = base_url
                    if item["href"].find("/tjsj") == 0:
                        base_url = f'http://www.stats.gov.cn{item["href"]}'
                        item["href"] = base_url
                    item["publisher_date"] = li.xpath('./a/span/font[2]/text()')[0]
                    item["to_oracle_date"] = datetime.datetime.now()
                    item["real_date"] = item["publisher_date"]
                    yield item
                except Exception as e:
                    traceback.print_exc(file=open('error.txt', 'a'))

    def parse_sse_newboard(self, response):
        '''
        上海证券交易所
        '''
        '''
        http://www.sse.com.cn/home/component/news/
        //*[@id="sse_list_1"]/dl/dd[1]/a
        //*[@id="sse_list_1"]/dl/dd[1]/span
        http://www.sse.com.cn/aboutus/mediacenter/hotandd/
        //*[@id="sse_list_1"]/dl/dd[1]/a
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        dd_list = selector.xpath('//*[@id="sse_list_1"]/dl/dd')
        for dd in dd_list:
            try:
                item["original"] = "上海证券交易所"
                item["original_id"] = 8
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./a/@title')[0]
                item["href"] = dd.xpath('./a/@href')[0]
                if item["href"].find("http://") != 0:
                    base_url = f'http://www.sse.com.cn{item["href"]}'
                    item["href"] = base_url
                item["publisher_date"] = dd.xpath('./span/text()')[0]
                item["to_oracle_date"] = datetime.datetime.now()
                item["real_date"] = item["publisher_date"]
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_szse_ss_newboard(self, response):
        '''
        深圳证券交易所
        '''
        '''
        http://www.szse.cn/aboutus/trends/news/
        /html/body/div[5]/div/div[2]/div/div[3]/div[1]/ul/li[1]/div/a
        /html/body/div[5]/div/div/div/div[2]/div[1]/ul/li[1]/div/script

        /html/body/div[5]/div/div/div/div[2]/div[1]/ul/li[1]/div/a
        /html/body/div[5]/div/div/div/div[2]/div[1]/ul/li[1]/div/span
        /html/body/div[5]/div/div[2]/div/div[3]/div[1]/ul/li[1]/div/a
        /html/body/div[5]/div/div/div/div[2]/div[1]/ul/li[1]/div/a
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div[5]/div/div/div/div[2]/div[1]/ul/li')
        for dd in li_list:
            try:
                item["original"] = "深圳证券交易所"
                item["original_id"] = 9
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./div/a/@title')[0]
                item["href"] = dd.xpath('./div/a/@href')[0]
                item["publisher_date"] = dd.xpath('./div/span/text()')[0].strip()
                item["to_oracle_date"] = datetime.datetime.now()
                item["real_date"] = item["publisher_date"]
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_szse_bs_newboard(self, response):
        '''
        深圳证券交易所
        '''
        '''
        http://www.szse.cn/aboutus/trends/news/
        /html/body/div[5]/div/div[2]/div/div[3]/div[1]/ul/li[1]/div/a
        /html/body/div[5]/div/div[2]/div/div[3]/div[1]/ul/li[1]/div/span
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div[5]/div/div[2]/div/div[3]/div[1]/ul/li')
        for dd in li_list:
            try:
                item["original"] = "深圳证券交易所"
                item["original_id"] = 9
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./div/a/@title')[0]
                item["href"] = dd.xpath('./div/a/@href')[0]
                if item["href"].find("./") == 0:
                    base_url = f'http://www.szse.cn/aboutus/trends/news{item["href"][1:]}'
                    item["href"] = base_url
                item["publisher_date"] = dd.xpath('./div/span/text()')[0].strip()
                item["to_oracle_date"] = datetime.datetime.now()
                item["real_date"] = item["publisher_date"]
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_news_politicspro_newboard(self, response):
        '''
        新华社
        '''
        '''
       http://www.news.cn/politicspro/
       http://www.news.cn/politics/2022-04/12/c_1128554030.htm
        //*[@id="mCSB_2"]/div[1]/div/div[1]/div[2]/span/a
        //*[@id="mCSB_2"]/div[1]/div
        //*[@id="mCSB_2"]/div[1]/div/div
        //*[@id="mCSB_1"]/div[1]/div/div[1]/div[2]/span/a
        //*[@id="mCSB_1"]/div[1]/div
        /html/body/div[3]/div[3]/div[5]/div[1]/div/div[1]/div
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        div_list = selector.xpath('//*[@id="recommendDepth"]/div/div/div')
        for dd in div_list:
            try:
                item["original"] = "新华社"
                item["original_id"] = 10
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./div[@class="tit"]/span/a/text()')[0]
                item["href"] = dd.xpath('./div[@class="tit"]/span/a/@href')[0]
                publisher_date = re.findall(r"http://www.news.cn/.*/(\d*-\d*/\d*)/.*", item["href"])
                if not publisher_date:
                    publisher_date = re.findall(r"http://www.news.cn/(\d*-\d*/\d*)/.*", item["href"])
                if publisher_date:
                    item["publisher_date"] = publisher_date[0]
                item["to_oracle_date"] = datetime.datetime.now()
                item["real_date"] = datetime.datetime.strptime(item["publisher_date"], "%Y-%m/%d").strftime(
                    "%Y-%m-%d")
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_news_fortunepro_newboard(self, response):
        '''
        新华社
        '''
        '''
       http://www.news.cn/fortunepro/
       //*[@id="mCSB_1"]/div[1]/div/div[1]/div/a
       //*[@id="mCSB_1"]/div[1]/div/div[1]/div/a
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        print(res)
        selector = etree.HTML(res)
        div_list = selector.xpath('//*[@id="mCSB_1"]/div[1]/div/div')
        for dd in div_list:
            try:
                item["original"] = "新华社"
                item["original_id"] = 10
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                if dd.xpath('./div[@class="tit"]/a/text()'):
                    item["title"] = dd.xpath('./div[@class="tit"]/a/text()')[0]
                    item["href"] = dd.xpath('./div[@class="tit"]/a/@href')[0]
                else:
                    item["title"] = dd.xpath('./div[@class="tit"]/div/a/text()')[0]
                    item["href"] = dd.xpath('./div[@class="tit"]/div/a/@href')[0]
                if item["href"].find("http://") != 0:
                    item["href"] = f'http://www.news.cn{item["href"]}'
                # item["publisher_date"] = dd.xpath('./a/div/em/text()')[0].strip()
                publisher_date = re.findall(r"http://www.news.cn/.*/(\d*-\d*/\d*)/.*", item["href"])
                if not publisher_date:
                    publisher_date = re.findall(r"http://www.news.cn/(\d*-\d*/\d*)/.*", item["href"])
                if not publisher_date:
                    publisher_date = re.findall(r"http://www.news.cn/.*/(\d*)/.*", item["href"])
                    if publisher_date:
                        publisher_date = [datetime.datetime.strptime(publisher_date[0], "%Y%m%d").strftime("%Y-%m/%d")]
                if publisher_date:
                    item["publisher_date"] = publisher_date[0]
                    item["real_date"] = datetime.datetime.strptime(item["publisher_date"],
                                                                   "%Y-%m/%d").strftime("%Y-%m-%d")

                # publisher_date = re.findall(r"http://www.news.cn/.*?/(.*)/.*", item["href"])
                # if publisher_date:
                #     item["publisher_date"] = publisher_date[0]
                #     if len(item["publisher_date"]) > 25:
                #         item["publisher_date"] = item["publisher_date"].split(r"/")[0]
                item["to_oracle_date"] = datetime.datetime.now()
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))
                # traceback.print_exc(file=open('error.txt', 'a'))

    def parse_news_fortunepro_newboard_v2(self, response):
        '''
        新华社
        '''
        '''
       http://www.news.cn/fortunepro/
       //*[@id="mCSB_1"]/div[1]/div/div[1]/div/a
       //*[@id="mCSB_1"]/div[1]/div/div[1]/div/a
        '''
        item = CsrcItem()
        for res in json.loads(response.body).get("data", {}).get("list", []):
            try:
                item["original_id"] = 10
                item["update_id"] = response.meta['_id']
                item["original_url"] = "http://www.news.cn/fortunepro"
                item["original"] = "新华社"
                item["title"] = res.get("Title")
                item["href"] = res.get("LinkUrl")
                item["publisher_date"] = res.get("PubTime")
                item["real_date"] = datetime.datetime.strptime(item["publisher_date"], "%Y-%m-%d %H:%M:%S").strftime(
                    "%Y-%m-%d")
                item["content"] = res.get("Abstract")
                item["to_oracle_date"] = datetime.datetime.now()
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_cs_hg_newboard(self, response):
        '''
        中证网
        '''

        '''
       'https://www.cs.com.cn/xwzx/hg/
        /html/body/div[3]/div/div/div[1]/ul/li[1]/a
        /html/body/div[3]/div/div/div[1]/ul/li[1]/a/div/em
        /html/body/div[3]/div/div/div[1]/ul/li[1]/a/h3

        '''
        item = CsrcItem()
        res = response.body.decode("gbk")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div[3]/div/div/div[1]/ul/li')
        for dd in li_list:
            try:
                item["original"] = "中证网"
                item["original_id"] = 11
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./a/h3/text()')[0]
                item["href"] = dd.xpath('./a/@href')[0]
                if item["href"].find("./") == 0:
                    base_url = f'https://www.cs.com.cn/xwzx/hg{item["href"][1:]}'
                    item["href"] = base_url
                item["publisher_date"] = dd.xpath('./a/div/em/text()')[0].strip()
                item["to_oracle_date"] = datetime.datetime.now()
                item["real_date"] = datetime.datetime.strptime(item["publisher_date"], "%Y-%m-%d %H:%M").strftime(
                    "%Y-%m-%d")
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_cnstock_news_yw_newboard(self, response):
        '''
        上海证券报
        '''
        '''
       https://news.cnstock.com/news/sns_yw/index.html
        //*[@id="j_waterfall_list"]/li
        

        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('//*[@id="j_waterfall_list"]/li')
        for dd in li_list:
            try:
                item["original"] = "上海证券报"
                item["original_id"] = 12
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./h2/a/text()')[0]
                item["href"] = dd.xpath('./h2/a/@href')[0]
                item["publisher_date"] = dd.xpath('./p/span/text()')[0].strip()
                item["to_oracle_date"] = datetime.datetime.now()
                if len(item["publisher_date"]) == 5:
                    item["real_date"] = datetime.datetime.now().strftime("%Y-%m-%d")
                elif len(item["publisher_date"]) == 11:
                    item["real_date"] = str(datetime.datetime.now().year) + "-" + item["publisher_date"]
                    item["real_date"] = datetime.datetime.strptime(item["real_date"], "%Y-%m-%d %H:%M").strftime(
                        "%Y-%m-%d")
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_cnstock_news_jg_newboard(self, response):
        '''
        上海证券报
        '''
        '''
       https://news.cnstock.com/news/sns_jg/index.html
        //*[@id="j_waterfall_list"]/li
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('//*[@id="j_waterfall_list"]/li')
        for dd in li_list:
            try:
                item["original"] = "上海证券报"
                item["original_id"] = 12
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./h2/a/text()')[0]
                item["href"] = dd.xpath('./h2/a/@href')[0]
                item["publisher_date"] = dd.xpath('./p/span/text()')[0].strip()
                item["to_oracle_date"] = datetime.datetime.now()
                if len(item["publisher_date"]) == 5:
                    item["real_date"] = datetime.datetime.now().strftime("%Y-%m-%d")
                elif len(item["publisher_date"]) == 11:
                    item["real_date"] = str(datetime.datetime.now().year) + "-" + item["publisher_date"]
                    item["real_date"] = datetime.datetime.strptime(item["real_date"], "%Y-%m-%d %H:%M").strftime(
                        "%Y-%m-%d")
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_stcn_xw_newboard(self, response):
        '''
        证券时报网
        '''

        '''
        https://www.stcn.com/xw/
        /html/body/div[3]/div[1]/dl/dd[1]/a
        //*[@id="idData"]/li[1]/p[1]/a
        //*[@id="idData"]/li[1]/p[3]
        //*[@id="idData"]/li[1]/p[3]/span
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        try:
            item["original"] = "证券时报网"
            item["original_id"] = 13
            item["update_id"] = response.meta['_id']
            item["original_url"] = response.url
            item["title"] = selector.xpath('/html/body/div[3]/div[1]/dl/dd[1]/a/@title')[0]
            item["href"] = selector.xpath('/html/body/div[3]/div[1]/dl/dd[1]/a/@href')[0]
            if item["href"].find("./") == 0:
                base_url = f'https://www.stcn.com/xw{item["href"][1:]}'
                item["href"] = base_url
            elif item["href"].find("../") == 0:
                base_url = f'https://www.stcn.com{item["href"][2:]}'
                item["href"] = base_url
            item["publisher_date"] = selector.xpath('/html/body/div[3]/div[1]/dl/dd[3]/text()')[0] + " " + \
                                     selector.xpath('/html/body/div[3]/div[1]/dl/dd[3]/span/text()')[0]
            item["real_date"] = selector.xpath('/html/body/div[3]/div[1]/dl/dd[3]/text()')[0]
            item["to_oracle_date"] = datetime.datetime.now()
            yield item
        except Exception as e:
            traceback.print_exc(file=open('error.txt', 'a'))

        li_list = selector.xpath('//*[@id="idData"]/li')
        for dd in li_list:
            try:
                item["original"] = "证券时报网"
                item["original_id"] = 13
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./p[1]/a/@title')[0]
                item["href"] = dd.xpath('./p[1]/a/@href')[0]
                if item["href"].find("./") == 0:
                    base_url = f'https://www.stcn.com/xw{item["href"][1:]}'
                    item["href"] = base_url
                elif item["href"].find("../") == 0:
                    base_url = f'https://www.stcn.com{item["href"][2:]}'
                    item["href"] = base_url
                item["publisher_date"] = dd.xpath('./p[3]/text()')[0].strip() + " " + dd.xpath('./p[3]/span/text()')[
                    0].strip()
                item["to_oracle_date"] = datetime.datetime.now()
                yield item
            except Exception as e:
                traceback.print_exc()

    def parse_stcn_kx_newboard(self, response):
        '''
        证券时报网
        '''

        '''
        http://www.stcn.com/kuaixun/
        //*[@id="news_list2"]/li[1]/i
        //*[@id="news_list2"]/li[1]/a
        //*[@id="news_list2"]/li[1]/span
        
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('//*[@id="news_list2"]/li')
        for dd in li_list:
            try:
                item["original"] = "证券时报网"
                item["original_id"] = 13
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./a/@title')[0]
                item["href"] = dd.xpath('./a/@href')[0]
                if item["href"].find("./") == 0:
                    base_url = f'https://www.stcn.com/kuaixun{item["href"][1:]}'
                    item["href"] = base_url
                elif item["href"].find("../") == 0:
                    base_url = f'https://www.stcn.com{item["href"][2:]}'
                    item["href"] = base_url
                item["publisher_date"] = dd.xpath('./span/text()')[0].strip() + " " + dd.xpath('./i/text()')[0].strip()
                item["real_date"] = dd.xpath('./span/text()')[0].strip()
                item["to_oracle_date"] = datetime.datetime.now()
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_stcn_kx_newboard_v2(self, response):
        '''
        证券时报网
        '''

        '''
        https://www.stcn.com/article/list/kx.html
        /html/body/div[2]/div[2]/div[1]/ul/li

        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div[2]/div[2]/div[1]/ul/li')
        for dd in li_list:
            try:
                item["original"] = "证券时报网"
                item["original_id"] = 13
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./div[@class="title"]/a/text()')[0]
                item["href"] = dd.xpath('./div[@class="title"]/a/@href')[0]
                if item["href"].find("./") == 0:
                    base_url = f'https://www.stcn.com/kuaixun{item["href"][1:]}'
                    item["href"] = base_url
                elif item["href"].find("../") == 0:
                    base_url = f'https://www.stcn.com{item["href"][2:]}'
                    item["href"] = base_url
                elif item["href"].find("/article") == 0:
                    base_url = f'https://www.stcn.com/{item["href"][1:]}'
                    item["href"] = base_url
                item["publisher_date"] = dd.xpath('./div[@class="time"]/text()')[0]
                item["publisher_date"] = f"{datetime.datetime.now().strftime('%Y-%m-%d')} {item['publisher_date']}"
                item["real_date"] = datetime.datetime.now().strftime("%Y-%m-%d")
                item["to_oracle_date"] = datetime.datetime.now()
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_zqrb_finance_newboard(self, response):
        '''
        证券日报网
        '''
        '''
       http://www.zqrb.cn/finance/hongguanjingji/index.html
       /html/body/div[2]/div[3]/div[1]/div[2]/ul/li[1]/a
        /html/body/div[2]/div[3]/div[1]/div[2]/ul/li[1]/span[1]
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div[2]/div[3]/div[1]/div[2]/ul/li')
        for dd in li_list:
            try:
                item["original"] = "证券日报网"
                item["original_id"] = 14
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./a/@title')[0]
                item["href"] = dd.xpath('./a/@href')[0]
                item["publisher_date"] = dd.xpath('./span/text()')[0].strip()
                item["real_date"] = datetime.datetime.strptime(item["publisher_date"], "%Y-%m-%d %H:%M").strftime(
                    "%Y-%m-%d")
                item["to_oracle_date"] = datetime.datetime.now()
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_zqrb_stock_newboard(self, response):
        '''
        证券日报网
        '''
        '''
       http://www.zqrb.cn/stock/gupiaoyaowen/index.html
       /html/body/div[2]/div[3]/div[1]/div[2]/ul/li[1]/a
        /html/body/div[2]/div[3]/div[1]/div[2]/ul/li[1]/span[1]
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div[2]/div[3]/div[1]/div[2]/ul/li')
        for dd in li_list:
            try:
                item["original"] = "证券日报网"
                item["original_id"] = 14
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./a/@title')[0]
                item["href"] = dd.xpath('./a/@href')[0]
                item["publisher_date"] = dd.xpath('./span/text()')[0].strip()
                item["to_oracle_date"] = datetime.datetime.now()
                item["real_date"] = datetime.datetime.strptime(item["publisher_date"], "%Y-%m-%d %H:%M").strftime(
                    "%Y-%m-%d")
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))

    def parse_zqrb_stock_red_newboard(self, response):
        '''
        证券日报网
        '''
        '''
      http://www.zqrb.cn/stock/redian/index.html
       /html/body/div[2]/div[3]/div[1]/div[2]/ul/li[1]/a
        /html/body/div[2]/div[3]/div[1]/div[2]/ul/li[1]/span[1]
        '''
        item = CsrcItem()
        res = response.body.decode("utf-8")
        selector = etree.HTML(res)
        li_list = selector.xpath('/html/body/div[2]/div[3]/div[1]/div[2]/ul/li')
        for dd in li_list:
            try:
                item["original"] = "证券日报网"
                item["original_id"] = 14
                item["update_id"] = response.meta['_id']
                item["original_url"] = response.url
                item["title"] = dd.xpath('./a/@title')[0]
                item["href"] = dd.xpath('./a/@href')[0]
                item["publisher_date"] = dd.xpath('./span/text()')[0].strip()
                item["to_oracle_date"] = datetime.datetime.now()
                item["real_date"] = datetime.datetime.strptime(item["publisher_date"], "%Y-%m-%d %H:%M").strftime(
                    "%Y-%m-%d")
                yield item
            except Exception as e:
                traceback.print_exc(file=open('error.txt', 'a'))
