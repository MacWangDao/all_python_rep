import argparse
import os
import traceback
from typing import AnyStr, List, Generator

from influxdb_client import InfluxDBClient
import pandas as pd

from config import load_configuration
"""
    --url:influxdb的连接地址
    --read_path:读取query文件的path
    --read_name:读取query文件的文件名
    --save_path:保存二进制文件路径
    --save_name:保存二进制文件名称。默认为read_name.pkl.zx
    --bat_read_path:批量读取文件路径
    --bat_save_path:批量写入文件路径
    
     eg. python save_bin_file.py --url http://192.168.101.201:8086 --read_path ./query --read_name query_1 --save_path ./save --save_name 123.1
"""

parser = argparse.ArgumentParser()


def create_dir(folder_name: str) -> None:
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


def get_files(path: str) -> Generator:
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


def get_data_frame_stream(url=None, token=None, org=None, query=None) -> Generator:
    with InfluxDBClient(url=url, token=token, org=org, timeout=60_000) as client:
        df_stream_generator = client.query_api().query_data_frame_stream(query, org=org)
        return df_stream_generator


def read_query(read_path: str, read_name: str) -> AnyStr:
    with open(os.path.join(read_path + "/", read_name), "r") as query:
        return query.read()


def save_pickle_file(df: pd.DataFrame, save_path: str, save_name: str) -> None:
    if df.empty:
        raise Exception("df.empty")
    create_dir(save_path)
    df.to_pickle(os.path.join(save_path + "/", save_name + ".pkl.xz"), compression="xz")
    print("to_pickle:" + os.path.join(save_path + "/", save_name + ".pkl.xz"))


def query_file_to_save(url, token, org, args) -> None:
    (filepath, tempfilename) = os.path.split(args.save_name)
    (filesname, extension) = os.path.splitext(tempfilename)
    args.save_name = filesname
    print(f"read:{os.path.join(args.read_path, args.read_name)}")
    query = read_query(args.read_path, args.read_name)
    print(query)
    if len(query) == 0:
        raise Exception("query file is empty:" + args.read_name)
    df_record_generator = get_data_frame_stream(url=url, token=token, org=org, query=query)
    for df_record in df_record_generator:
        if df_record is None:
            raise Exception(query)
        print(f"shape:{df_record.shape}")
        save_pickle_file(df_record, args.save_path, args.save_name)


def init_save_file() -> None:
    try:
        config = load_configuration()
        token = config.get("token")
        org = config.get("org")
        url = config.get("url")
        read_path = config.get("read_path")
        read_name = config.get("read_name")
        save_path = config.get("save_path")
        save_name = config.get("save_name")
        parser.add_argument('--url', help='http://192.168.101.201:8086', type=str,
                            default='http://192.168.101.201:8086')
        parser.add_argument('--host', help='influxdb ip', type=str, default='127.0.0.1')
        parser.add_argument('--org', help='org', type=str, default=org)
        parser.add_argument('--read_path', help='read_path', type=str, default=read_path)
        parser.add_argument('--read_name', help='read_name', type=str, default=read_name)
        parser.add_argument('--save_path', help='save_path', type=str, default=save_path)
        parser.add_argument('--save_name', help='save_name', type=str, default=save_name)
        parser.add_argument('--bat_read_path', help='bat_read_path', type=str)
        parser.add_argument('--bat_save_path', help='bat_save_path', type=str)
        args = parser.parse_args()
        if not args.url:
            raise Exception(f"args.url:{args.url}")
        if not args.org:
            raise Exception(f"args.org:{args.org}")

        if args.bat_read_path:
            files_generator = get_files(args.bat_read_path)
            for query_file in files_generator:
                (filepath, tempfilename) = os.path.split(query_file)
                args.read_path = filepath
                args.read_name = tempfilename
                args.save_name = tempfilename
                args.save_path = args.bat_save_path
                query_file_to_save(url, token, org, args)
        else:
            if not args.read_path:
                raise Exception(f"args.read_path:{args.read_path}")
            if not args.read_name:
                raise Exception(f"args.read_name:{args.read_name}")
            if not args.save_path:
                raise Exception(f"args.save_path:{args.save_path}")
            if not args.save_name:
                (filepath, tempfilename) = os.path.split(args.read_name)
                (filesname, extension) = os.path.splitext(tempfilename)
                args.save_name = filesname
            if not os.path.exists(args.read_path):
                raise Exception(f"not exists args.read_path:{args.read_path}")
            query_file_to_save(url, token, org, args)


    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    """
    --url:influxdb的连接地址
    --read_path:读取query文件的path
    --read_name:读取query文件的文件名
    --save_path:保存二进制文件路径
    --save_name:保存二进制文件名称。默认为read_name.pkl.zx
    --bat_read_path:批量读取文件路径
    --bat_save_path:批量写入文件路径
    
     eg. python save_bin_file.py --url http://192.168.101.201:8086 --read_path ./query --read_name query_1 --save_path ./save --save_name 123.1
    """
    init_save_file()
