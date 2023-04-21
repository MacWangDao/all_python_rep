import time
from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
from loguru import logger
import random

from ths_index_industry import rank_index_industry_time_share
from write_influxdb_realtime import realtime_to_influxdb
from write_oracle_realtime import realtime_to_oracle


def main():
    mon = 10
    day_list = ["31"]
    for day in day_list:
        date = pd.date_range(start=f'2022-{mon}-{day} 09:30:00', end=f'2022-{mon}-{day} 15:00:00', freq='min')
        rest_time = pd.date_range(start=f'2022-{mon}-{day} 11:30:00', end=f'2022-{mon}-{day} 12:59:00', freq='min')
        rest_time = rest_time.tolist()
        for date_time in date.tolist():
            if date_time not in rest_time:
                logger.info("date_time")
                find_date = date_time.strftime("%Y%m%d %H:%M")
                df = rank_index_industry_time_share(find_date=find_date)
                if df is not None:
                    with ThreadPoolExecutor(max_workers=3) as pool:
                        future1 = pool.submit(realtime_to_oracle, df)
                        future2 = pool.submit(realtime_to_influxdb, df)
                time.sleep(random.choice([2, 3, 4, 5]))


if __name__ == "__main__":
    main()
