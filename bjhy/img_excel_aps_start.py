import os
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler

import platform

from all_hold_stock_num_new_1012 import run_all_stock
from img_excel_sftp_new_1012 import run_img_excel_sftp

scheduler = BlockingScheduler(timezone='Asia/Shanghai')
# sched = BackgroundScheduler(timezone='Asia/Shanghai')
'''
second
minute
'''
"""
year：四位数的年份
month：1-12之间的数字或字符串，如果不指定，则为*，表示每个月
day：1-31，如果不指定，则为*，表示每一天
week：1-53，如果不指定，则为*，表示每一星期
day_of_week：一周有7天，用0-6表示，比如指定0-3，则表示周一到周四。不指定则为7天，也可以用          mon,tue,wed,thu,fri,sat,sun表示
hour：0-23
minute：0-59
second：0-59
start_date：起始时间
end_date：结束时间
timezone：时区
jitter：随机的浮动秒数
当省略时间参数时，在显式指定参数之前的参数会被设定为*，表示每(月、天)xxx。之后的参数会被设定为最小值，week 和day_of_week的最小值为*。
比如，设定day=10等同于设定year='*', month='*', day=1, week='*', day_of_week='*', hour=0, minute=0, second=0，
即每个月的第10天触发。为什么是每个月而不是每个星期，注意参数位置，week被放在了后面。day后面的参数hour、minute、second则被设置为0。因此不仅是每个月的第10天触发，还是每个月的第10天的00:00:00的时候触发
"""
tradedate = None


# @scheduler.scheduled_job("cron", day_of_week="0-5", hour=8, minute=35, second=0, id='task1', max_instances=10)
# def job_first():
#     run_img_excel_sftp(tradedate)
@scheduler.scheduled_job("cron", day_of_week="1-5", hour=8, minute=35, second=2, id='task1', max_instances=3,
                         misfire_grace_time=10)
def job_first():
    run_img_excel_sftp(tradedate)


# @scheduler.scheduled_job("cron", day_of_week="0-5", hour="9", minute="20", id='task2', max_instances=10)
# def job_second():
#     run_img_excel_sftp(tradedate)


# @scheduler.scheduled_job("cron", day_of_week="0-5", hour=8, minute=35, second=0, id='task3', max_instances=10)
# def job_third():
#     run_all_stock(tradedate)
@scheduler.scheduled_job("cron", day_of_week="1-5", hour=8, minute=35, second=5, id='task3', max_instances=3,
                         misfire_grace_time=10)
def job_third():
    run_all_stock(tradedate)


#
#
# @scheduler.scheduled_job("cron", day_of_week="0-5", hour="9", minute="20", id='task4', max_instances=10)
# def job_forth():
#     run_all_stock(tradedate)


if __name__ == "__main__":
    if platform.system().lower() == "linux":
        os.environ["PATH"] = os.environ.get("PATH", "") + ":" + "/home/toptrade/anaconda3/envs/selpy39/bin"
    scheduler.start()
