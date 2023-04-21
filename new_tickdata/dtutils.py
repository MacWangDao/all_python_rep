import pandas as pd
import datetime

'''
def tdfdate_to_pddate(tdfdate):
    merged_datetime = tdfdate
    formated_datetime = merged_datetime[0:4] + '-' + \
        merged_datetime[4:6] + '-' + \
        merged_datetime[6:8]
    return pd.to_datetime(formated_datetime,format="%Y-%m-%d")


def tdftime_to_pdtime( tdftime):
    merged_datetime = tdftime.zfill(9) 
    formated_datetime = merged_datetime[8:10] + '-' + \
        merged_datetime[10:12] + '-' + \
        merged_datetime[12:14] + '-' + \
        merged_datetime[14:17]
    return pd.to_datetime(formated_datetime,format="%H-%M-%S-%f")
'''


def tdfdt_to_pddt(tdfdate, tdftime, use_utc=True, tz_str='+0800'):
    if use_utc:
        merged_datetime = tdfdate + tdftime.zfill(9)
        formated_datetime = merged_datetime[0:4] + '-' + \
                            merged_datetime[4:6] + '-' + \
                            merged_datetime[6:8] + '-' + \
                            merged_datetime[8:10] + '-' + \
                            merged_datetime[10:12] + '-' + \
                            merged_datetime[12:14] + '-' + \
                            merged_datetime[14:17] + ' ' + tz_str
        return pd.to_datetime(formated_datetime, format="%Y-%m-%d-%H-%M-%S-%f %z", utc=True)
    else:
        merged_datetime = tdfdate + tdftime.zfill(9)
        formated_datetime = merged_datetime[0:4] + '-' + \
                            merged_datetime[4:6] + '-' + \
                            merged_datetime[6:8] + '-' + \
                            merged_datetime[8:10] + '-' + \
                            merged_datetime[10:12] + '-' + \
                            merged_datetime[12:14] + '-' + \
                            merged_datetime[14:17]
        return pd.to_datetime(formated_datetime, format="%Y-%m-%d-%H-%M-%S-%f", utc=False)


def add_pddt_index(df, use_utc=True, tz_str='+0800'):
    # df['date'].astype('str').str
    if use_utc:
        merged_datetime = df['date'] + df['tdftime'].str.zfill(9)
        formated_datetime = merged_datetime.str[0:4] + '-' + \
                            merged_datetime.str[4:6] + '-' + \
                            merged_datetime.str[6:8] + '-' + \
                            merged_datetime.str[8:10] + '-' + \
                            merged_datetime.str[10:12] + '-' + \
                            merged_datetime.str[12:14] + '-' + \
                            merged_datetime.str[14:17] + ' ' + tz_str

        # df['datetime'] =
        df.index = pd.to_datetime(formated_datetime, format="%Y-%m-%d-%H-%M-%S-%f %z", utc=True)
        df.index.name = 'ts'
    else:
        merged_datetime = df['date'] + df['tdftime'].str.zfill(9)
        formated_datetime = merged_datetime.str[0:4] + '-' + \
                            merged_datetime.str[4:6] + '-' + \
                            merged_datetime.str[6:8] + '-' + \
                            merged_datetime.str[8:10] + '-' + \
                            merged_datetime.str[10:12] + '-' + \
                            merged_datetime.str[12:14] + '-' + \
                            merged_datetime.str[14:17]

        # df['datetime'] =
        df.index = pd.to_datetime(formated_datetime, format="%Y-%m-%d-%H-%M-%S-%f", utc=False)
        df.index.name = 'ts'

    return df


# check = is_hour_between('08:30:00', '04:29:00') #spans to the next day
def is_hour_between(ts, start, end):
    # Format the datetime string
    target_ts = ts.timetz()
    time_format = '%H:%M:%S %z'
    # Convert the start and end datetime to just time
    start = datetime.datetime.strptime(start, time_format).timetz()
    end = datetime.datetime.strptime(end, time_format).timetz()

    is_between = False
    is_between |= start <= target_ts <= end
    is_between |= end <= start and (start <= target_ts or target_ts <= end)

    return is_between


def get_datetime_str(style='dt'):
    cur_time = datetime.datetime.now()

    date_str = cur_time.strftime('%y%m%d')
    time_str = cur_time.strftime('%H%M%S')

    if style == 'data':
        return date_str
    elif style == 'time':
        return time_str
    else:
        return date_str + '_' + time_str


if __name__ == '__main__':
    d1 = tdfdt_to_pddt('20220325', '91740000')
    d2 = tdfdt_to_pddt('20220325', '114040000')
    print(d1, d2)
    print(is_hour_between(d1, '09:16:03 +0800', '09:18:57 +0800'))
    print(is_hour_between(d1, '09:17:03 +0800', '09:20:57 +0800'))
    print(not is_hour_between(d1, '09:18:03 +0800', '09:20:57 +0800'))

    print(is_hour_between(d2, '11:30:03 +0800', '13:00:00 +0800'))
    print(is_hour_between(d2, '11:25:03 +0800', '0:38:57 +0800'))  # next day
    print(not is_hour_between(d2, '12:25:03 +0800', '11:38:57 +0800'))  # next day

    print(get_datetime_str())
