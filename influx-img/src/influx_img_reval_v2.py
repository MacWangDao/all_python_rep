import datetime

from influxdb_client import InfluxDBClient
import numpy as np
import matplotlib.pyplot as plt
import io
import pandas as pd
from PIL import Image
import cv2 as cv
from sklearn.preprocessing import MinMaxScaler
from config import load_configuration
import os
import traceback
from sqlalchemy import create_engine
import cx_Oracle as oracle
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

# You can generate a Token from the "Tokens Tab" in the UI
from math import sqrt


def isPrime(n):
    for i in range(2, int(sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True


def get_max_prime(num):
    index = 2
    maxPrime = None

    while index <= num:
        if isPrime(index) and num % index == 0:
            num /= index
            maxPrime = index
        index += 1
    return maxPrime


def get_data_frame_stream(url=None, token=None, org=None, query=None):
    with InfluxDBClient(url=url, token=token, org=org, timeout=60_000) as client:
        df_stream_generator = client.query_api().query_data_frame_stream(query, org=org)
        return df_stream_generator


def format_data_frame(df_record):
    df_record = df_record[["_time", "_value", "_field", "_measurement", "instrument", "price"]]
    df_record = df_record[["_time", "_value", "price"]]
    df_record['_value'] = df_record['_value'].astype(np.int64)
    df_record['price'] = df_record['price'].astype(np.float64)
    df_record.set_index("_time", drop=True, append=False, inplace=True)
    df_record = df_record.apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    array = df_record.to_numpy()
    return array


def format_data_frame_array(df):
    df['price'] = df['price'].astype(np.float64)
    df = df.apply(lambda d: d.astype(np.float64))
    df = df.apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    array = df.to_numpy()
    return array


def format_data_frame_array_color(arry):
    return arry * 255


def fit_transform_data(array):
    data = MinMaxScaler().fit_transform(array)
    return data


def array_to_img(array):
    array *= 255
    array = np.reshape(array, (-1, 16))
    plt.imshow(array, cmap=plt.get_cmap('gray'))
    plt.show()


def buffer_img(buffer, array):
    x, y = array.shape
    # plt.ImageFile.MAXBLOCK = x * y
    plt.imsave(buffer, array, cmap=plt.get_cmap('gray'), format="jpeg")
    image_data = buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    with open("./img/img_3.jpeg", "wb") as imgf:
        imgf.write(image_data)


def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


def buffer_matplot_img_png(buffer, array, out_path, img_name):
    create_folder(out_path)
    plt.imsave(buffer, array, cmap=plt.get_cmap('gray'), format="png")
    image_data = buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    print(img_name)
    with open(out_path + "/" + img_name + ".png", "wb") as imgf:
        imgf.write(image_data)


def buffer_cv2_img_png(buffer, array, out_path, img_name):
    create_folder(out_path)
    cv.imwrite(out_path + "/" + img_name + ".png", array)
    # image_data = buffer.getvalue()
    # buffer.seek(0)
    # buffer.truncate(0)
    # print(img_name)
    # with open(out_path + "/" + img_name + ".png", "wb") as imgf:
    #     imgf.write(image_data)


def buffer_matplot_img_jpeg(buffer, array):
    x, y = array.shape
    # plt.ImageFile.MAXBLOCK = x * y
    plt.imsave(buffer, array, cmap=plt.get_cmap('gray'), format="jpeg")
    image_data = buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    with open("./img/img_7.jpeg", "wb") as imgf:
        imgf.write(image_data)


def buffer_pil_img(array):
    image = Image.fromarray(array, mode="L")
    # image.save("./img/img_4.jpeg", format="jpeg")
    image.save("./img/img_4.png", format="png")


def buffer_pil_img_png(array):
    image = Image.fromarray(array, mode="L")
    # image.save("./img/img_4.jpeg", format="jpeg")
    image.save("./img/img_5.png", format="png")


def buffer_pil_img_buffer(buffer, array, out_path, img_name, mode="L", format="png"):
    create_folder(out_path)
    image = Image.fromarray(array)
    image = image.convert(mode)
    image.save(buffer, format=format)
    image_data = buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    with open(out_path + "/" + img_name + f".{format}", "wb") as imgf:
        imgf.write(image_data)


def buffer_pil_img_buffer_jpeg(buffer, array):
    image = Image.fromarray(array, mode="L")
    image.save(buffer, format="jpeg")
    image_data = buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    with open("./img/img_6.jpeg", "wb") as imgf:
        imgf.write(image_data)


def rescale(arr):
    arr_min = arr.min()
    arr_max = arr.max()
    return (arr - arr_min) / (arr_max - arr_min)


def to_timestamp(arr):
    return arr.timestamp()


def get_datetime_list(start=None, end=None, freq=None):
    s = pd.date_range(start=start, end=end, freq=freq)
    return list(map(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"), s.tolist()))


def get_datetime_list_periods(start=None, end=None):
    s = pd.date_range(start=start, end=end, periods=2)
    return tuple(map(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"), s.tolist()))


def bucket_query_list(bucket, date_ts, instrument, sp_value_list, time_interval):
    if not date_ts:
        raise Exception("date_ts")
    timeRangeStart, timeRangeStop = date_ts
    path_time = datetime.datetime.strptime(timeRangeStop, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H%M%SZ")
    img_name = instrument + "-" + path_time
    for sp_value in sp_value_list:
        img_name = instrument + "-" + sp_value
        query = f"""
                from(bucket: "{bucket}")
                    |> range(start: {timeRangeStart}, stop:{timeRangeStop})
                    |> filter(fn: (r) =>
                        (r["_measurement"] == "{sp_value}"))
                    |> filter(fn: (r) =>
                        (r["_field"] == "value"))
                    |> filter(fn: (r) =>
                        (r["instrument"] == "{instrument}"))
                    |> window(every: {time_interval}s)
                    |> mean()
                    |> duplicate(column:"_stop",as:"_time")
                    |> window(every:inf)
                    |> drop(columns: ["result","_start", "_stop", "table", "_field","_measurement","instrument"])
                """
        yield query, img_name, sp_value


def record_dataframe(df_record, y, sp_value):
    # df_record = df_record[df_record["_measurement"] == sp_value][["_time", "_value", "price"]]
    df_record = df_record[["_time", "_value", "price"]].copy()
    df_record.columns = ["_time", sp_value, "price"]
    df_record['price'] = df_record['price'].astype(np.float64)
    df_record['price_range'] = pd.cut(x=df_record["price"], bins=y, right=True)
    df_record = df_record.groupby(["_time", "price_range"])[sp_value].mean().reset_index()
    array_list = []
    for name, group in df_record.groupby(["_time"]):
        group[sp_value] = group[[sp_value]].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
        group[sp_value].fillna(0, inplace=True)
        array = group[sp_value].to_numpy()
        array = array.reshape(len(array), -1)
        array_list.append(array)
    arrys = np.concatenate(array_list, axis=1)
    return arrys, df_record[["_time", "price_range"]]


def sp_ask_value(x, y, sp_value):
    A_zero = np.zeros([x, y])
    A_zero = A_zero.ravel()
    A = sp_value.to_numpy().ravel()
    print(sp_value.to_numpy().shape)
    if len(A_zero) >= len(A):
        A_zero[:len(A)] = A[:]
    else:
        A_zero[:] = A[:len(A_zero)]
    A_zero = A_zero.reshape([x, y])
    return A_zero


def get_y_value(lowlimited, highlimited, y):
    """
    7.28 10.92

    7.03 10.55
    """
    y = np.linspace(lowlimited, highlimited, y)
    y_list = []

    for index, val in enumerate(y):
        if index + 1 < len(y):
            y_list.append([y[index], y[index + 1]])
    return y


def format_parameters(code, trade_date):
    trade_date = trade_date.strftime("%Y%m%d")
    code = code.split(".")[1]
    return code, trade_date


def db_data(db_config, db_code, db_trade_date):
    host = db_config.get('host')
    user = db_config.get('user')
    port = db_config.get('port')
    password = db_config.get('password')
    service_name = db_config.get('service_name')
    dns = oracle.makedsn(host, port, service_name=service_name)
    engine = create_engine(f"oracle://{user}:{password}@" + dns, encoding='utf-8', echo=True)
    sql = f"SELECT TRADE_DATE,CODE,LOWLIMITED,HIGHLIMITED FROM T_O_SNAPSHOT_VALUE WHERE TRADE_DATE={db_trade_date} AND CODE={db_code} AND rownum<=1"
    res_df = pd.read_sql(sql, engine)
    print(res_df)
    res = res_df.to_dict(orient="list")
    lowlimited = res.get("lowlimited")
    lowlimited = lowlimited[0] if len(lowlimited) > 0 else None
    highlimited = res.get("highlimited")
    highlimited = highlimited[0] if len(highlimited) > 0 else None
    return lowlimited, highlimited


def to_desc_file(df, out_path, csv_name):
    file_path = out_path + "/" + csv_name + ".csv"
    time_range = df[["_time"]].drop_duplicates(ignore_index=True).T
    price_range = df[["price_range"]].drop_duplicates(ignore_index=True).T
    tp_range = pd.concat([time_range, price_range])
    tp_range.to_csv(file_path, header=False)


def run_v2():
    try:
        config = load_configuration()
        x = config.get("x")
        y = config.get("y")
        token = config.get("token")
        org = config.get("org")
        bucket = config.get("bucket")
        url = config.get("url")
        timeRangeStart = config.get("timeRangeStart")
        timeRangeStop = config.get("timeRangeStop")
        instruments = config.get("instruments")
        instruments = list(set(instruments))
        time_interval = config.get("time_interval")
        out_path = config.get("out_path")
        sp_values = config.get("sp_values")
        db_config = config.get("db_config")
        date_path = timeRangeStart.strftime("%Y-%m-%d")
        out_path = os.path.join(out_path, date_path)
        date_ts = get_datetime_list_periods(start=timeRangeStart, end=timeRangeStop)
        for code in instruments:
            db_code, db_trade_date = format_parameters(code, timeRangeStart)
            lowlimited, highlimited = db_data(db_config, db_code, db_trade_date)
            if lowlimited is None:
                raise Exception("lowlimited")

            out_path = os.path.join(out_path + "/", code)
            query_generator = bucket_query_list(bucket, date_ts, code, sp_values, time_interval)
            for query, img_name, sp_value in query_generator:
                try:
                    print(query)
                    df_record_generator = get_data_frame_stream(url=url, token=token, org=org, query=query)
                    for df_record in df_record_generator:
                        if df_record is None:
                            raise Exception(query)
                        print(df_record[["_time", "_value", "price"]].shape)
                        y_list = get_y_value(lowlimited, highlimited, y)
                        arry, df_record = record_dataframe(df_record, y_list, sp_value)
                        arry = format_data_frame_array_color(arry)
                        buffer = io.BytesIO()
                        buffer_matplot_img_png(buffer, arry, out_path, img_name)
                        to_desc_file(df_record, out_path, img_name)
                except Exception:
                    traceback.print_exc()
    except Exception:
        traceback.print_exc()


def multiproces_pool_img():
    config = load_configuration()
    x = config.get("x")
    y = config.get("y")
    token = config.get("token")
    org = config.get("org")
    bucket = config.get("bucket")
    url = config.get("url")
    timeRangeStart = config.get("timeRangeStart")
    timeRangeStop = config.get("timeRangeStop")
    instruments = config.get("instruments")
    instruments = list(set(instruments))
    time_interval = config.get("time_interval")
    out_path = config.get("out_path")
    sp_values = config.get("sp_values")
    db_config = config.get("db_config")
    date_path = timeRangeStart.strftime("%Y-%m-%d")
    out_path = os.path.join(out_path, date_path)
    date_ts = get_datetime_list_periods(start=timeRangeStart, end=timeRangeStop)
    kargs = {"timeRangeStart": timeRangeStart, "db_config": db_config, "out_path": out_path, "bucket": bucket,
             "date_ts": date_ts, "sp_values": sp_values, "time_interval": time_interval, "url": url,
             "token": token, "org": org, "y": y}
    with ThreadPoolExecutor(max_workers=len(instruments) + 1) as executor_first:
        for code in instruments:
            executor_first.submit(innner_method_process, (code), **kargs)


def innner_method_process(code, timeRangeStart, db_config, out_path, bucket, date_ts, sp_values, time_interval, url,
                          token, org, y):
    db_code, db_trade_date = format_parameters(code, timeRangeStart)
    lowlimited, highlimited = db_data(db_config, db_code, db_trade_date)
    if lowlimited is None:
        raise Exception("lowlimited")
    out_path_inner = os.path.join(out_path + "/", code)
    query_generator = bucket_query_list(bucket, date_ts, code, sp_values, time_interval)
    with ProcessPoolExecutor(max_workers=4) as executor_secend:
        for query, img_name, sp_value in query_generator:
            inner_kargs = {"img_name": img_name, "sp_value": sp_value, "url": url, "token": token,
                           "org": org, "lowlimited": lowlimited, "highlimited": highlimited, "y": y,
                           "out_path_inner": out_path_inner}
            executor_secend.submit(inner_img_process, (query), **inner_kargs)


def inner_img_process(query, img_name, sp_value, url, token, org, lowlimited, highlimited, y, out_path_inner):
    print(query)
    try:
        df_record_generator = get_data_frame_stream(url=url, token=token, org=org, query=query)
        for df_record in df_record_generator:
            if df_record is None:
                raise Exception(query)
            print(df_record[["_time", "_value", "price"]].shape)
            y_list = get_y_value(lowlimited, highlimited, y)
            arry, df_record = record_dataframe(df_record, y_list, sp_value)
            arry = format_data_frame_array_color(arry)
            buffer = io.BytesIO()
            buffer_matplot_img_png(buffer, arry, out_path_inner, img_name)
            to_desc_file(df_record, out_path_inner, img_name)
    except Exception:
        traceback.print_exc()


def main():
    # run_v2()
    multiproces_pool_img()


if __name__ == "__main__":
    main()
