import asyncio

import aiohttp
import requests

# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
url = "https://sc.hkexnews.hk/TuniS/www3.hkexnews.hk/sdw/search/searchsdw_c.aspx"

# proxies = {
#     "http": "http://10.10.1.10:3128",
#     "https": "http://10.10.1.10:1080",
# }

# r = requests.get('https://www.baidu.com/', proxies=proxies)

headers = {
    "authority": "www3.hkexnews.hk",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "cache-control": "max-age=0",
    'content-type': 'application/x-www-form-urlencoded',
    'cookie': 'sclang=zh-HK; WT_FPC=id=23.43.249.170-843578560.30970140:lv=1657695841691:ss=1657695841691',
    # 'origin': 'https://www3.hkexnews.hk',
    'origin': 'https://sc.hkexnews.hk',
    # 'referer': 'https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx',
    'referer': "https://sc.hkexnews.hk/TuniS/www3.hkexnews.hk/sdw/search/searchsdw_c.aspx",
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
    'sec-ch-ua-mobile': '?0',
    "sec-ch-ua-platform": '"Windows"',
    'sec-fetch-dest': "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"
}

data = {"__EVENTTARGET": "btnSearch",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": "/wEPDwUKMTY0ODYwNTA0OWRkM79k2SfZ+VkDy88JRhbk+XZIdM0=",
        "__VIEWSTATEGENERATOR": "3B50BBBD",
        "today": "20220715",
        "sortBy": "shareholding",
        "sortDirection": "desc",
        "alertMsg": "",
        "txtShareholdingDate": "2022/06/30",
        "txtStockCode": "90999",
        # "txtStockName": "招商證券",
        "txtParticipantID": "",
        "txtParticipantName": "",
        "txtSelPartID": ""}


async def fetch_v2(url):
    proxy_ip = "http://202.55.5.209:8090"
    # proxy_ip = None
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
        async with session.post(url, timeout=5,
                                proxy=proxy_ip, data=data, headers=headers) as response:
            return await response.text(), response.status


async def network(url):
    tasks = []
    for _ in range(1000):
        task = asyncio.ensure_future(fetch_v2(url))
        tasks.append(task)

    await asyncio.wait(tasks)


def agent_1():
    proxies = {
        "http": "http://218.201.77.143:9091",
    }
    response = requests.post(url, proxies=proxies, data=data, headers=headers)
    print(response.status_code)


if __name__ == "__main__":
    # agent_1()
    asyncio.run(network(url))
