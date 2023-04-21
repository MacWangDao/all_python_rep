import os
# from scrapy import cmdline
from apscheduler.schedulers.blocking import BlockingScheduler
import platform

# from twisted.internet import reactor

# from csrc.spiders.csrc_news_info import CsrcNewsInfoSpider

scheduler = BlockingScheduler()

# from scrapy.crawler import CrawlerProcess, CrawlerRunner

# from scrapy.utils.project import get_project_settings
#
# settings = get_project_settings()
# crawler = CrawlerProcess(settings)

'''
second
minute
'''


@scheduler.scheduled_job("cron", day_of_week="0-4", minute="*/1", hour="9-15", id='task', max_instances=100)
def job():
    os.system("scrapy crawl csrc_news_info ")
    # cmdline.execute("scrapy crawl csrc_news_info ".split())


if __name__ == "__main__":
    if platform.system().lower() == "linux":
        os.environ["PATH"] = os.environ.get("PATH", "") + ":" + "/home/toptrade/anaconda3/envs/selpy39/bin"
    scheduler.start()
