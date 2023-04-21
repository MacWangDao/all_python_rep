from influxdb_client import InfluxDBClient
import numpy as np
import matplotlib.pyplot as plt
import io
import pandas as pd
from PIL import Image
import PIL

buffer = io.BytesIO()

# You can generate a Token from the "Tokens Tab" in the UI
token = "pFGIYU6RQZ6bfObi0HD3B3KIF4xYq5IesSzvGVkSo675-6BPXk3phh0SUhVhi-_U93OBC1YhT_j-BQvwqNsDpQ=="
org = "bjhy"
bucket = "quote-v2"
url = "http://192.168.101.201:8086"

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
    with InfluxDBClient(url=url, token=token, org=org) as client:
        df_stream = client.query_api().query_data_frame_stream(query, org=org)
        for df_record in df_stream:
            return df_record


def format_data_frame(df_record):
    df_record = df_record[["_time", "_value", "_field", "_measurement", "instrument", "price"]]
    df_record = df_record[["_time", "_value", "price"]]
    df_record['_value'] = df_record['_value'].astype(np.int64)
    df_record['price'] = df_record['price'].astype(np.float64)
    df_record.set_index("_time", drop=True, append=False, inplace=True)
    df_record = df_record.apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    # df_record.loc[:, ['_value']].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    # df_record.loc[:, ['price']].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    # df_record.loc[:, ['_value']].apply(lambda x: x.min() - x.max())
    # df_record.loc[:, ['price']].apply(lambda x: x.min() - x.max())
    # print(df_record.loc[:, ['price']].apply(lambda x: x+1))
    # print(df_record['price'])
    # df_record.apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    # print(df_record.head(5))
    array = df_record.to_numpy()
    return array


def format_data_frame_array(df):
    df['price'] = df['price'].astype(np.float64)
    df = df.apply(lambda d: d.astype(np.float64))
    df = df.apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    array = df.to_numpy()
    return array


def array_to_img(array):
    # print(array.shape)
    # print(array[0])
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


def buffer_pil_img(array):
    image = Image.fromarray(array, mode="L")
    # image.save("./img/img_4.jpeg", format="jpeg")
    image.save("./img/img_4.png", format="png")


def buffer_pil_img_png(array):
    image = Image.fromarray(array, mode="L")
    # image.save("./img/img_4.jpeg", format="jpeg")
    image.save("./img/img_5.png", format="png")


def buffer_pil_img_buffer(buffer, array):
    image = Image.fromarray(array, mode="L")
    image.save(buffer, format="png")
    image_data = buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    with open("./img/img_5.png", "wb") as imgf:
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


if __name__ == "__main__":
    query = """option v = {timeRangeStart: 2022-04-06T02:34:12Z, timeRangeStop: 2022-04-06T02:34:13Z}

        from(bucket: "quote-v2")
            |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
            |> filter(fn: (r) =>
                (r["_measurement"] == "sp_ask_amt" or r["_measurement"] == "sp_ask_vol"))
            |> filter(fn: (r) =>
                (r["_field"] == "value"))"""
    query = """option v = {timeRangeStart: 2022-04-06T02:34:12Z, timeRangeStop: 2022-04-06T03:34:13Z}

            from(bucket: "quote-v2")
                |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
                |> filter(fn: (r) =>
                    (r["_measurement"] == "sp_ask_amt" or r["_measurement"] == "sp_ask_vol"))
                |> filter(fn: (r) =>
                    (r["instrument"] == "sz.300024" or r["instrument"] == "sz.300026" or r["instrument"] == "sz.300037"))"""
    query = """option v = {timeRangeStart: 2022-04-06T02:34:12Z, timeRangeStop: 2022-04-06T04:35:18Z}

                       from(bucket: "quote-v2")
                       	|> range(start: v.timeRangeStart, stop: v.timeRangeStop)
                       	|> filter(fn: (r) =>
                       		(r["_measurement"] == "sp_ask_amt" or r["_measurement"] == "sp_ask_vol"))
                       	|> filter(fn: (r) =>
                       		(r["instrument"] == "sz.300024"))"""
    query = """option v = {timeRangeStart: 2022-04-06T02:00:00Z, timeRangeStop: 2022-04-06T02:03:00Z}

        from(bucket: "quote-v2")
            |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
            |> filter(fn: (r) =>
                (r["_measurement"] == "sp_ask_amt" or r["_measurement"] == "sp_ask_vol" or r["_measurement"] == "sp_bid_amt" or r["_measurement"] == "sp_bid_vol"))
            |> filter(fn: (r) =>
                (r["_field"] == "value"))
            |> filter(fn: (r) =>
                (r["instrument"] == "sz.300024"))"""
    df_record = get_data_frame_stream(url=url, token=token, org=org, query=query)
    sp_ask_amt = df_record[df_record["_measurement"] == "sp_ask_amt"][["_time", "_value", "price"]]
    sp_ask_amt.columns = ["_time", "sp_ask_amt_value", "price"]
    # sp_ask_amt.set_index(["_time"], drop=True, append=False, inplace=True)
    sp_ask_vol = df_record[df_record["_measurement"] == "sp_ask_vol"][["_time", "_value", "price"]]
    sp_ask_vol.columns = ["_time", "sp_ask_vol_value", "price"]
    # sp_ask_vol.set_index(["_time"], drop=True, append=False, inplace=True)
    sp_bid_amt = df_record[df_record["_measurement"] == "sp_bid_amt"][["_time", "_value", "price"]]
    sp_bid_amt.columns = ["_time", "sp_bid_amt_value", "price"]
    # sp_bid_amt.set_index(["_time"], drop=True, append=False, inplace=True)
    sp_bid_vol = df_record[df_record["_measurement"] == "sp_bid_vol"][["_time", "_value", "price"]]
    sp_bid_vol.columns = ["_time", "sp_bid_vol_value", "price"]
    # sp_bid_vol.set_index(["_time"], drop=True, append=False, inplace=True)
    df = pd.merge(sp_ask_amt, sp_ask_vol, how='inner', left_on=["_time", "price"],
                  right_on=["_time", "price"])
    df = pd.merge(df, sp_bid_amt, how='inner', left_on=["_time", "price"],
                  right_on=["_time", "price"])
    df = pd.merge(df, sp_bid_vol, how='inner', left_on=["_time", "price"],
                  right_on=["_time", "price"])
    # df.set_index(["_time"], drop=True, append=False, inplace=True)
    df["_time"] = df["_time"].apply(lambda x: x.timestamp())
    # print(type(df.index.tolist()[0]))
    # print(df.index.tolist()[0])
    # for t in df.index.tolist():
    #     print(t.timestamp())
    # print(df["sp_ask_amt_value"])
    # print(df["sp_ask_vol_value"])
    # print(df["sp_bid_amt_value"])
    # print(df["sp_bid_vol_value"])
    # df = pd.DataFrame(columns=["_time"])
    # df.set_index(["_time"], drop=True, append=False, inplace=True)
    # df = pd.merge(sp_ask_amt, sp_ask_vol, how='inner', left_index=True, right_index=True)
    # df = pd.merge(df, sp_ask_vol, how='inner', left_index=True, right_index=True)
    # df = pd.merge(df, sp_bid_amt, how='inner', left_index=True, right_index=True)
    # df = pd.merge(df, sp_bid_vol, how='inner', left_index=True, right_index=True)
    # print(df[["_time", "sp_ask_amt_value", "price"]])
    A_zero = np.zeros([40, 4800])
    A_zero = A_zero.ravel()
    A = df[["sp_ask_amt_value"]].to_numpy().ravel()
    print(df[["sp_ask_amt_value"]].to_numpy().shape)
    if len(A_zero) >= len(A):
        A_zero[:len(A)] = A[:]
    else:
        A_zero[:] = A[:len(A_zero)]
    A_zero = A_zero.reshape([40, 4800])
    print(A_zero)

    # print(sp_ask_amt)
    # print(sp_ask_vol)

    array = format_data_frame(df_record)
    x, y = array.shape
    prime = get_max_prime(x * y)
    print(prime)
    # array_to_img(array)
    # array = np.reshape(array, (prime, -1))
    buffer_img(buffer, array * 255)
    # print(array * 255)
    # buffer_pil_img(array * 255)

    arr = format_data_frame_array(df)
    print(arr.shape)
    x, y = arr.shape
    prime = get_max_prime(x * y)
    # print(prime)
    # array_to_img(array)
    arr = np.reshape(arr, (-1, prime))
    arr *= 255
    # print(arr)
    buffer_pil_img_png(arr)
    buffer_pil_img_buffer_jpeg(buffer, arr)
