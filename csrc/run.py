from scrapy import cmdline

# cmdline.execute("scrapy crawl csrc_enent".split())
#scrapy crawl csrc_news_info --nolog
cmdline.execute("scrapy crawl csrc_news_info ".split())
