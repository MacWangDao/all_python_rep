import os
from apscheduler.schedulers.blocking import BlockingScheduler

import platform

from img_excel_sftp_hk_data_1201 import Hksftpdata

scheduler = BlockingScheduler(timezone='Asia/Shanghai')

tradedate = None


@scheduler.scheduled_job("cron", day_of_week="1-5", hour=6, minute=20, second=2, id='task_hk_1', max_instances=3,
                         misfire_grace_time=10)
def hk_to_excel_job():
    hk = Hksftpdata(tradedate)
    hk.__str__()


# @scheduler.scheduled_job("cron", day_of_week="1-5", hour=2, minute=40, second=5, id='task3', max_instances=3,
#                          misfire_grace_time=10)
# def job_third():
#     pass
#     # run_all_stock(tradedate)


if __name__ == "__main__":
    if platform.system().lower() == "linux":
        os.environ["PATH"] = os.environ.get("PATH", "") + ":" + "/home/toptrade/anaconda3/envs/selpy39/bin"
    scheduler.start()
