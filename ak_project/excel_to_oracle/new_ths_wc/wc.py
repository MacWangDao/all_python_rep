import os

import pandas as pd
import requests
import py_mini_racer


def _get_file_content_ths(file: str = "ths.js") -> str:
    current_path = os.path.dirname(os.path.abspath(__file__))
    # father_path = os.path.dirname(current_path)
    path = os.path.join(current_path, file)

    """
    获取 JS 文件的内容
    :param file:  JS 文件名
    :type file: str
    :return: 文件内容
    :rtype: str
    """

    with open(path) as f:
        file_data = f.read()
    return file_data


url = "http://www.iwencai.com/unifiedwap/unified-wap/v2/result/get-robot-data"
js_code = py_mini_racer.MiniRacer()
js_content = _get_file_content_ths("ths.js")
js_code.eval(js_content)
v_code = js_code.call("v")
headers = {
    "hexin-v": v_code,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
}
find_date = "20230320"
params = {
    "question": f"{find_date} 所有股票行业分类;主营;概念",
    "perpage": "5000",
    "page": "2",
    "secondary_intent": "stock",
    "log_info": '{"input_type":"typewrite"}',
    "source": "Ths_iwencai_Xuangu",
    "version": "2.0",
    "query_area": "",
    "block_list": "",
    # "rsh": "Ths_iwencai_Xuangu_hst1pkx5cuv8giz4dpoy6ay08ccl2zyt",
    "add_info": '{"urp":{"scene":1,"company":1,"business":1},"contentType":"json","searchInfo":true}',
}
r = requests.post(url, data=params, headers=headers, timeout=5)
data_json = r.json()
data = data_json["data"]["answer"][0]["txt"][0]["content"]["components"][0]["data"]["datas"]
res = pd.DataFrame(data)
print(res[["股票代码", "股票简称"]])
