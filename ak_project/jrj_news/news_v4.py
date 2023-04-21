import asyncio
import concurrent
import datetime
import json
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import aiohttp
import pandas as pd
import pytz
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup
from loguru import logger
from lxml import etree
import sys

sys.setrecursionlimit(100000)
logger.add("log/jrj_news_v4.log", rotation="100MB", encoding="utf-8", enqueue=True,
           format="{time:YYYY-MM-DD HH:mm:ss} |  {name} | {line} | {message}", retention="10 days")

shanghai = pytz.timezone('Asia/Shanghai')
scheduler = BlockingScheduler(timezone=shanghai)


class JrjNews(object):
    def __init__(self):
        pass

    async def get_urls(self, session, start_url):
        headers = {
            "sec-ch-ua": '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Sec-Fetch-Dest:": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        }
        urls = []
        async with session.get(start_url, headers=headers, verify_ssl=False, timeout=10) as response:
            try:
                html = await response.text(encoding="gb2312", errors='ignore')
                selector = etree.HTML(html)
                li_list = selector.xpath('/html/body/div/div[2]/ul/li')
                for li in li_list:
                    if len(li.xpath('./a/@href')) > 0:
                        uri = li.xpath('./a/@href')[0]
                        if uri.startswith("//"):
                            url = f"https:{uri}"
                            urls.append(url)
                return urls
            except Exception as e:
                logger.error(start_url)
                logger.error(e)
                return urls

    async def get_urls_for_bs4(self, session, start_url):
        headers = {
            "sec-ch-ua": '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Sec-Fetch-Dest:": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        }
        urls = []
        timeout = aiohttp.ClientTimeout(total=20)
        async with session.get(start_url, headers=headers, verify_ssl=False, timeout=timeout) as response:
            try:
                html = await response.text(encoding="gb2312", errors='ignore')
                soup = BeautifulSoup(html, 'html.parser')
                ul_list = soup.find("ul", class_="list")
                if ul_list:
                    for li in ul_list:
                        if li.__class__.__name__ == "Tag":
                            a_hrefs = li.find_all("a", recursive=False)
                            if a_hrefs:
                                for href in a_hrefs:
                                    url = href.get("href", "")
                                    if url.strip():
                                        if url.startswith("//"):
                                            url = f"http:{url}"
                                        urls.append(url)
                return urls
            except Exception as e:
                logger.error(start_url)
                logger.error(e)
                return urls

    async def fetch_url_data(self, session, url):
        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with session.get(url, timeout=timeout) as response:
                text = await response.text(encoding="gb2312", errors='ignore')
                url = response.url
                return text, url
        except Exception as e:
            logger.error(e)
            return ()

    async def start_jrj_allnews(self, start: str = "2015-01-01", start_url: str = "", tfund: str = ""):
        try:
            timeout = aiohttp.ClientTimeout(total=600)
            connector = aiohttp.TCPConnector(limit=50)
            now = datetime.datetime.today().strftime("%Y-%m-%d")
            async with aiohttp.ClientSession(connector=connector, trust_env=True, timeout=timeout) as session:
                news_dates = pd.date_range(start=now, end=now, freq='D')
                # news_dates = pd.date_range(start="2023-04-08", end=now, freq='D')
                news_dates = news_dates.tolist()
                news_dates.sort(reverse=True)
                for news_date in news_dates:
                    news_date = news_date.strftime('%Y%m%d')
                    news_url = f"{start_url}/{news_date[:-2]}/{news_date}_1.shtml"
                    urls = await self.get_urls_for_bs4(session, news_url)
                    tasks = []
                    for url in urls:
                        if url == "http://www.jrj.com.cn/error.shtml" or url == "":
                            continue
                        task = asyncio.ensure_future(self.fetch_url_data(session, url))
                        tasks.append(task)
                    responses = await asyncio.gather(*tasks)
                    if responses:
                        if len(responses) == 0:
                            continue
                        tasks_2 = []
                        for response in responses:
                            if len(response) == 2:
                                text, url = response
                                task2 = asyncio.ensure_future(
                                    self.parse_responses_async_bank_hk_forex_usstock_finance_stock_allnews(text, url,
                                                                                                           session,
                                                                                                           news_date=news_date,
                                                                                                           tfund=tfund))
                                tasks_2.append(task2)
                        if len(tasks_2) > 0:
                            await asyncio.wait(tasks_2)
        except Exception as e:
            logger.exception(e)

    def write_txet(self, file_name: str = None, out_path: str = None, text: str = None):
        try:
            os.makedirs(out_path, exist_ok=True)
            out_path_file = os.path.join(out_path, file_name)
            with open(out_path_file, 'w') as f:
                f.write(text)
                logger.info(out_path_file)
        except Exception as e:
            logger.exception(e)

    async def download_img_location(self, session, url: str = None, out_path: str = None):
        try:
            (filepath, tempfilename) = os.path.split(url)
            (filename, extension) = os.path.splitext(tempfilename)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
            }
            if url == "":
                return
            async with session.get(url, headers=headers, timeout=30) as img:
                content = await img.read()
                os.makedirs(out_path, exist_ok=True)
                out_path_file = os.path.join(out_path, tempfilename)
                with open(out_path_file, 'wb') as f:
                    f.write(content)
                    logger.info(url)
        except aiohttp.ClientConnectorError as e:
            logger.warning(e)
            logger.error(url)
        except aiohttp.ServerTimeoutError as e:
            logger.warning(e)
            logger.error(url)
        except asyncio.TimeoutError as e:
            logger.error(e)
            logger.error(url)
        except Exception as e:
            logger.error(url)
            logger.exception(e)

    async def parse_responses_async_bank_hk_forex_usstock_finance_stock_allnews(self, text, url, session,
                                                                                news_source: str = "金融界",
                                                                                news_date: str = None,
                                                                                tfund: str = ""):
        try:
            out_path = f"save_json/{news_source}/{tfund}/{news_date}"
            soup = BeautifulSoup(text, 'html.parser')
            if soup.title is None:
                return
            file_name = soup.title.get_text().strip()
            file_name = file_name.replace("/", "")
            json_file_name = f"{file_name}.json"
            img_dir = f"{file_name}-img"
            img_out_path = os.path.join(out_path, img_dir)
            content = ""
            articles_date = {
                "url": "",
                "news_source": "",
                "title": "",
                "article_source": "",
                "news_date_time": "",
                "keywords": "",
                "content": "",
                "recommend": "",
                "imgs": []
            }
            kws = soup.find('meta', attrs={"name": "keywords"})
            if kws:
                keywords = kws.get("content")
                articles_date.update({"keywords": keywords})
            articles_date.update({"title": file_name, "url": str(url), "news_source": news_source})
            logger.info(url)
            titmains = soup.find("div", class_="text-col")
            if titmains:
                for titmain in titmains:
                    if titmain.name == "h1":
                        title = titmain.get_text().strip()
                        articles_date.update({"title": title})
                    elif titmain.name == "p":
                        date_time_source_auth = titmain.find("span")
                        if date_time_source_auth:
                            date_time_source_auths = date_time_source_auth.get_text().strip().split("\n")
                            if len(date_time_source_auths) == 1:
                                date_time = date_time_source_auths[0]
                                articles_date.update({"news_date_time": date_time})
                            elif len(date_time_source_auths) == 2:
                                date_time = date_time_source_auths[0]
                                source = date_time_source_auths[1]
                                articles_date.update({"news_date_time": date_time, "article_source": source})
                            elif len(date_time_source_auths) == 3:
                                date_time = date_time_source_auths[0]
                                source = date_time_source_auths[1]
                                auth = date_time_source_auths[2]
                                articles_date.update(
                                    {"news_date_time": date_time, "article_source": source, "article_auth": auth})
                    elif titmain.name == "div":
                        texttit_m1_class = titmain.attrs.get("class", [])
                        if texttit_m1_class:
                            if len(texttit_m1_class) > 0:
                                if texttit_m1_class[0] == "newsource":
                                    date_time = titmain.find('span', id='pubtime_baidu')
                                    source = titmain.find('span', id='source_baidu')
                                    auth = titmain.find('span', id='author_baidu')
                                    if date_time:
                                        date_time = date_time.get_text().strip()
                                        articles_date.update({"news_date_time": date_time})
                                    if source:
                                        source = source.get_text().strip()
                                        articles_date.update({"article_source": source})
                                    if auth:
                                        auth = auth.get_text().strip()
                                        articles_date.update({"article_auth": auth})
                                elif texttit_m1_class[0] == "textmain":
                                    imgs = []
                                    for article in titmain:
                                        if article.name == "p":
                                            content += article.text
                                            img_tags = article.find_all('img')
                                            for img in img_tags:
                                                img_url = img.get("src", "")
                                                if img_url.strip():
                                                    if img_url.startswith("//"):
                                                        img_url = f"http:{img_url}"
                                                    (filepath, img_filename) = os.path.split(img_url)
                                                    content += f"\n<img src={img_out_path}/{img_filename}>"
                                                    imgs.append(img_url)
                                                    await self.download_img_location(session, img_url, img_out_path)
                                        elif article.name == "span":
                                            span_title = article.get("title")
                                            if span_title:
                                                content += f"\n{span_title}"
                                                img_tags = article.find_all('img')
                                                for img in img_tags:
                                                    img_url = img.get("src", "")
                                                    if img_url.strip():
                                                        if img_url.startswith("//"):
                                                            img_url = f"http:{img_url}"
                                                        (filepath, img_filename) = os.path.split(img_url)
                                                        content += f"\n<img src={img_out_path}/{img_filename}>"
                                                        imgs.append(img_url)
                                                        await self.download_img_location(session, img_url, img_out_path)
                                        elif article.name == "table":
                                            tbody = article.find("tbody")
                                            if tbody:
                                                th_list = []
                                                trs = tbody.find_all('tr')
                                                for ind, tr in enumerate(trs):
                                                    ths = tr.find_all('th')
                                                    for th in ths:
                                                        th_list.append(th.get_text())
                                                    if ind == 0:
                                                        content += f"\n{','.join(th_list)}"
                                                    tds = tr.find_all('td')
                                                    td_list = []
                                                    for index, td in enumerate(tds):
                                                        td_list.append(td.get_text())
                                                    content += f"\n{','.join(td_list)}"
                                            else:
                                                trs = article.find_all("tr")
                                                if trs:
                                                    for tr in trs:
                                                        content += f"\n{tr.get_text()}"
                                        elif article.name == "div":
                                            div_class = article.attrs.get("class", [])
                                            if len(div_class) > 0:
                                                if div_class[0] == "textimg":
                                                    recommend = article.get_text().strip()
                                                    articles_date.update({"recommend": recommend})
                                        else:
                                            table = article.find("table")
                                            if table.__class__.__name__ == "Tag":
                                                th_list = []
                                                trs = table.find_all('tr')
                                                for ind, tr in enumerate(trs):
                                                    ths = tr.find_all('th')
                                                    for th in ths:
                                                        th_list.append(th.get_text())
                                                    if ind == 0:
                                                        content += f"\n{','.join(th_list)}"
                                                    tds = tr.find_all('td')
                                                    td_list = []
                                                    for index, td in enumerate(tds):
                                                        td_list.append(td.get_text())
                                                    content += f"\n{','.join(td_list)}"
                                            content += "\n"
                                    articles_date.update({"imgs": imgs})
                articles_date.update({"content": content})
            else:
                titmains = soup.find("div", class_="newsContainer")
                if titmains:
                    newsLeft = titmains.find("div", id="leftnews")
                    for titmain in newsLeft:
                        if titmain.__class__.__name__ == "Tag":
                            texttit_m1_class = titmain.attrs.get("class", [])
                            if len(texttit_m1_class) > 0:
                                if texttit_m1_class[0] == "newsConTit":
                                    title = titmain.get_text().strip()
                                    articles_date.update({"title": title})
                            if titmain.name == "h1":
                                title = titmain.get_text().strip()
                                articles_date.update({"title": title})
                            elif titmain.name == "p":
                                date_time_source_auths = titmain.get_text().strip().split("\n")
                                date_time_source_auths = date_time_source_auths[:3]
                                if len(date_time_source_auths) == 3:
                                    date_time = date_time_source_auths[0]
                                    source = "".join(date_time_source_auths[1:])
                                    articles_date.update({"news_date_time": date_time, "article_source": source})
                            elif titmain.name == "div":
                                newsCon_id = titmain.attrs.get("id")
                                if newsCon_id == "IDNewsDtail":
                                    imgs = []
                                    for article in titmain:
                                        if article.name == "p":
                                            content += article.text
                                            img_tags = article.find_all('img')
                                            for img in img_tags:
                                                img_url = img.get("src", "")
                                                if img_url.strip():
                                                    if img_url.startswith("//"):
                                                        img_url = f"http:{img_url}"
                                                    (filepath, img_filename) = os.path.split(img_url)
                                                    content += f"\n<img src={img_out_path}/{img_filename}>"
                                                    imgs.append(img_url)
                                                    await self.download_img_location(session, img_url, img_out_path)
                                        elif article.name == "span":
                                            span_title = article.get("title")
                                            if span_title:
                                                content += f"\n{span_title}"
                                                img_tags = article.find_all('img')
                                                for img in img_tags:
                                                    img_url = img.get("src", "")
                                                    if img_url.strip():
                                                        if img_url.startswith("//"):
                                                            img_url = f"http:{img_url}"
                                                        (filepath, img_filename) = os.path.split(img_url)
                                                        content += f"\n<img src={img_out_path}/{img_filename}>"
                                                        imgs.append(img_url)
                                                        await self.download_img_location(session, img_url, img_out_path)
                                        elif article.name == "table":
                                            tbody = article.find("tbody")
                                            if tbody:
                                                th_list = []
                                                trs = tbody.find_all('tr')
                                                for ind, tr in enumerate(trs):
                                                    ths = tr.find_all('th')
                                                    for th in ths:
                                                        th_list.append(th.get_text())
                                                    if ind == 0:
                                                        content += f"\n{','.join(th_list)}"
                                                    tds = tr.find_all('td')
                                                    td_list = []
                                                    for index, td in enumerate(tds):
                                                        td_list.append(td.get_text())
                                                    content += f"\n{','.join(td_list)}"
                                            else:
                                                trs = article.find_all("tr")
                                                if trs:
                                                    for tr in trs:
                                                        content += f"\n{tr.get_text()}"
                                        elif article.name == "div":
                                            div_class = article.attrs.get("class", [])
                                            if len(div_class) > 0:
                                                if div_class[0] == "textimg":
                                                    recommend = article.get_text().strip()
                                                    articles_date.update({"recommend": recommend})
                                        else:
                                            table = article.find("table")
                                            if table.__class__.__name__ == "Tag":
                                                th_list = []
                                                trs = table.find_all('tr')
                                                for ind, tr in enumerate(trs):
                                                    ths = tr.find_all('th')
                                                    for th in ths:
                                                        th_list.append(th.get_text())
                                                    if ind == 0:
                                                        content += f"\n{','.join(th_list)}"
                                                    tds = tr.find_all('td')
                                                    td_list = []
                                                    for index, td in enumerate(tds):
                                                        td_list.append(td.get_text())
                                                    content += f"\n{','.join(td_list)}"
                                            content += "\n"
                                    articles_date.update({"imgs": imgs})
                                othersLink_class = titmain.attrs.get("class", [])
                                if len(othersLink_class) > 0:
                                    if othersLink_class[0] == "othersLink":
                                        if titmain.p:
                                            auth = titmain.p.get_text().strip()
                                            articles_date.update({"article_auth": auth})
                    articles_date.update({"content": content})
                else:
                    titmains = soup.find("div", class_="titmain")
                    if titmains:
                        for titmain in titmains:
                            if titmain.name == "h1":
                                title = titmain.get_text().strip()
                                articles_date.update({"title": title})
                            elif titmain.name == "p":
                                date_time_source_auth = titmain.find("span")
                                if date_time_source_auth:
                                    date_time_source_auths = date_time_source_auth.get_text().strip().split("\n")
                                    if len(date_time_source_auths) == 1:
                                        date_time = date_time_source_auths[0]
                                        articles_date.update({"news_date_time": date_time})
                                    elif len(date_time_source_auths) == 2:
                                        date_time = date_time_source_auths[0]
                                        source = date_time_source_auths[1]
                                        articles_date.update({"news_date_time": date_time, "article_source": source})
                                    elif len(date_time_source_auths) == 3:
                                        date_time = date_time_source_auths[0]
                                        source = date_time_source_auths[1]
                                        auth = date_time_source_auths[2]
                                        articles_date.update(
                                            {"news_date_time": date_time, "article_source": source,
                                             "article_auth": auth})
                            elif titmain.name == "div":
                                texttit_m1 = titmain.get("class")
                                if texttit_m1:
                                    if len(texttit_m1) > 0:
                                        if texttit_m1[0] == "texttit_m1":
                                            imgs = []
                                            for article in titmain:
                                                if article.name == "p":
                                                    content += article.text
                                                    img_tags = article.find_all('img')
                                                    for img in img_tags:
                                                        img_url = img.get("src", "")
                                                        if img_url.strip():
                                                            if img_url.startswith("//"):
                                                                img_url = f"http:{img_url}"
                                                            (filepath, img_filename) = os.path.split(img_url)
                                                            content += f"\n<img src={img_out_path}/{img_filename}>"
                                                            imgs.append(img_url)
                                                            await self.download_img_location(session, img_url,
                                                                                             img_out_path)
                                                elif article.name == "span":
                                                    # print(article.attrs)
                                                    span_title = article.get("title")
                                                    if span_title:
                                                        content += f"\n{span_title}"
                                                        img_tags = article.find_all('img')
                                                        for img in img_tags:
                                                            img_url = img.get("src", "")
                                                            if img_url.strip():
                                                                if img_url.startswith("//"):
                                                                    img_url = f"http:{img_url}"
                                                                (filepath, img_filename) = os.path.split(img_url)
                                                                content += f"\n<img src={img_out_path}/{img_filename}>"
                                                                imgs.append(img_url)
                                                                await self.download_img_location(session, img_url,
                                                                                                 img_out_path)
                                                elif article.name == "table":
                                                    tbody = article.find("tbody")
                                                    if tbody:
                                                        th_list = []
                                                        trs = tbody.find_all('tr')
                                                        for ind, tr in enumerate(trs):
                                                            ths = tr.find_all('th')
                                                            for th in ths:
                                                                th_list.append(th.get_text())
                                                            if ind == 0:
                                                                content += f"\n{','.join(th_list)}"
                                                            tds = tr.find_all('td')
                                                            td_list = []
                                                            for index, td in enumerate(tds):
                                                                td_list.append(td.get_text())
                                                            content += f"\n{','.join(td_list)}"
                                                    else:
                                                        trs = article.find_all("tr")
                                                        if trs:
                                                            for tr in trs:
                                                                content += f"\n{tr.get_text()}"
                                                else:
                                                    table = article.find("table")
                                                    if table.__class__.__name__ == "Tag":
                                                        th_list = []
                                                        trs = table.find_all('tr')
                                                        for ind, tr in enumerate(trs):
                                                            ths = tr.find_all('th')
                                                            for th in ths:
                                                                th_list.append(th.get_text())
                                                            if ind == 0:
                                                                content += f"\n{','.join(th_list)}"
                                                            tds = tr.find_all('td')
                                                            td_list = []
                                                            for index, td in enumerate(tds):
                                                                td_list.append(td.get_text())
                                                            content += f"\n{','.join(td_list)}"
                                                    content += "\n"
                                            articles_date.update({"imgs": imgs})
                        articles_date.update({"content": content})
                    else:
                        texttitboxs = soup.find("div", class_="texttitbox")
                        if texttitboxs is None:
                            logger.warning("no data --------------------------------")
                            logger.error(url)
                            return
                        for texttitbox in texttitboxs:
                            if texttitbox.name == "h1":
                                news_title = texttitbox.get_text().strip()
                                articles_date.update({"title": news_title})
                            elif texttitbox.name == "div":
                                date_time_source = texttitbox.find("div", class_="mh-title")
                                if date_time_source:
                                    date_time_source_auth = date_time_source.find("span", class_="time")
                                    date_time_source_auths = date_time_source_auth.get_text().strip().split("\n")
                                    if len(date_time_source_auths) == 1:
                                        date_time = date_time_source_auths[0]
                                        articles_date.update({"news_date_time": date_time})
                                    elif len(date_time_source_auths) == 2:
                                        date_time = date_time_source_auths[0]
                                        source = date_time_source_auths[1]
                                        articles_date.update({"news_date_time": date_time, "article_source": source})
                                    elif len(date_time_source_auths) == 3:
                                        date_time = date_time_source_auths[0]
                                        source = date_time_source_auths[1]
                                        auth = date_time_source_auths[2]
                                        articles_date.update(
                                            {"news_date_time": date_time, "article_source": source,
                                             "article_auth": auth})

                                articles = texttitbox.find("div", class_="texttit_m1")
                                if articles:
                                    imgs = []
                                    for article in articles:
                                        if article.name == "p":
                                            content += f"\n{article.get_text()}"
                                            img_tags = article.find_all('img')
                                            for img in img_tags:
                                                img_url = img.get("src", "")
                                                if img_url.strip():
                                                    if img_url.startswith("//"):
                                                        img_url = f"http:{img_url}"
                                                    (filepath, img_filename) = os.path.split(img_url)
                                                    content += f"\n<img src={img_out_path}/{img_filename}>"
                                                    imgs.append(img_url)
                                                    await self.download_img_location(session, img_url, img_out_path)
                                        elif article.name == "span":
                                            span_title = article.get("title")
                                            if span_title:
                                                content += f"\n{span_title}"
                                                img_tags = article.find_all('img')
                                                for img in img_tags:
                                                    img_url = img.get("src", "")
                                                    if img_url.strip():
                                                        if img_url.startswith("//"):
                                                            img_url = f"http:{img_url}"
                                                        (filepath, img_filename) = os.path.split(img_url)
                                                        content += f"\n<img src={img_out_path}/{img_filename}>"
                                                        imgs.append(img_url)
                                                        await self.download_img_location(session, img_url, img_out_path)
                                        elif article.name == "table":
                                            tbody = article.find("tbody")
                                            if tbody:
                                                th_list = []
                                                trs = tbody.find_all('tr')
                                                for ind, tr in enumerate(trs):
                                                    ths = tr.find_all('th')
                                                    for th in ths:
                                                        th_list.append(th.get_text())
                                                    if ind == 0:
                                                        content += f"\n{','.join(th_list)}"
                                                    tds = tr.find_all('td')
                                                    td_list = []
                                                    for index, td in enumerate(tds):
                                                        td_list.append(td.get_text())
                                                    content += f"\n{','.join(td_list)}"
                                            else:
                                                trs = article.find_all("tr")
                                                if trs:
                                                    for tr in trs:
                                                        content += f"\n{tr.get_text()}"
                                        else:
                                            if article.__class__.__name__ == 'Tag':
                                                span_text = article.find("span", class_="mh-title")
                                                if span_text:
                                                    content += f"\n{span_text.get_text()}"
                                            table = article.find("table")
                                            if table.__class__.__name__ == "Tag":
                                                th_list = []
                                                trs = table.find_all('tr')
                                                for ind, tr in enumerate(trs):
                                                    ths = tr.find_all('th')
                                                    for th in ths:
                                                        th_list.append(th.get_text())
                                                    if ind == 0:
                                                        content += f"\n{','.join(th_list)}"
                                                    tds = tr.find_all('td')
                                                    td_list = []
                                                    for index, td in enumerate(tds):
                                                        td_list.append(td.get_text())
                                                    content += f"\n{','.join(td_list)}"
                                            content += "\n"
                                    articles_date.update({"imgs": imgs})
                        articles_date.update({"content": content})
            json_str = json.dumps(articles_date, ensure_ascii=False)
            self.write_txet(json_file_name, out_path, json_str)
        except Exception as e:
            logger.error(url)
            logger.exception(e)

    def _main(self, start_url: str = None, tfund: str = ""):
        try:
            asyncio.run(self.start_jrj_allnews(start_url=start_url, tfund=tfund))
        except Exception as e:
            logger.exception(e)

    def run(self):
        try:
            tfunds = ["银行", "港股", "美股", "外汇", "财经", "股票", "基金"]
            jrj_news_urls = ["http://bank.jrj.com.cn/xwk", "http://hk.jrj.com.cn/xwk", "http://usstock.jrj.com.cn/xwk",
                             "http://forex.jrj.com.cn/xwk", "http://finance.jrj.com.cn/xwk",
                             "http://stock.jrj.com.cn/xwk", "http://fund.jrj.com.cn/xwk"]
            # ThreadPoolExecutor
            # ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=8) as executor:
                futures = []
                for start_url, tf in zip(jrj_news_urls, tfunds):
                    future = executor.submit(self._main, start_url, tf)
                    futures.append(future)
                concurrent.futures.wait(futures)
        except Exception as e:
            logger.exception(e)


@scheduler.scheduled_job("cron", day_of_week="0-6", hour="8,12", minute="20", second="5", id='jrj01',
                         max_instances=3,
                         misfire_grace_time=10, timezone=shanghai)
@scheduler.scheduled_job("cron", day_of_week="0-6", hour="16,22", minute="10", second="5", id='jrj02',
                         max_instances=3,
                         misfire_grace_time=10, timezone=shanghai)
def start():
    jrj = JrjNews()
    jrj.run()


if __name__ == "__main__":
    scheduler.start()
    # jrj = JrjNews()
    # jrj.run()
