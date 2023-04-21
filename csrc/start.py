from scrapy.crawler import CrawlerProcess
# from scrapy.utils.project import get_project_settings
#
# settings = get_project_settings()
#
# crawler = CrawlerProcess(settings)
# crawler.crawl('crawlall')
# # crawler.crawl('csrc_enent')
# crawler.start()

# from scrapy import cmdline
#
# cmdline.execute("scrapy crawlall".split())
import os
import subprocess


def run():
    os.system("scrapy crawl csrc_news --nolog")
    os.system("scrapy crawl csrc_enent --nolog")
    # process = subprocess.Popen("scrapy crawl csrc_news", shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
    #                            stderr=subprocess.PIPE)
    # print(process.communicate())
    # process = subprocess.Popen("scrapy crawl csrc_enent", shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
    #                            stderr=subprocess.PIPE)
    # print(process.communicate())



if __name__ == "__main__":
    run()
