# encoding=utf8

'''
1 每天qa，big_search的recall
2 每天qa无法提取实体词的比例

其中1，分子是成功推荐了的（succeed in recomnd)，分母：①所有请求的uid ②
'''

import sys
import time

from bdp_utils.cy_bdp.service import BDPManager
from general_utils.time_utils import get_today_timestamp, timestamp2datetime, timestamp2date
from general_utils.file_utils import pickle_from_file
from recommend.test_scripts.analysis_logs import get_yesterday_log_filename

TABLE_NAME = "recommend_log_analysis"


def create_table():
    schema = [
        {
            "name": "id",  # 字段名称
            "type": "number",  # 类型只能是number, string, date三种类型
        },
        {
            "name": "created_date",  # 数据插入的时间
            "type": "date",  # datetime格式的例如 '2017-07-08 12:01:00'
        },
        ################
        {
            "name": "news_all_input_qa",  # 所有请求的uid的量
            "type": "number",
        },
        {
            "name": "news_all_output_qa",  # 成功计算出文章id的量
            "type": "number",
        },
        {
            "name": "news_no_info_qa",  # 查不到信息的量（无效请求）
            "type": "number",
        },
        {
            "name": "news_filtered_by_preprocessing_qa",  # 预处理规则（filter_user_info）过滤掉的的量
            "type": "number",
        },
        {
            "name": "news_empty_res_qa",  # 文章rank之后，文章都被过滤掉，的量
            "type": "number",
        },
        {
            "name": "news_bad_res_qa",  # 后处理之后，啥也没剩 或者 最高分低于阈值的量，
            "type": "number",
        },
        ################
        {
            "name": "news_all_input_bs",
            "type": "number",
        },
        {
            "name": "news_all_output_bs",
            "type": "number",
        },
        {
            "name": "news_no_info_bs",
            "type": "number",
        },
        {
            "name": "news_filtered_by_preprocessing_bs",
            "type": "number",
        },
        {
            "name": "news_empty_res_bs",  # 文章rank之后，文章都被过滤掉，的量
            "type": "number",
        },
        {
            "name": "news_bad_res_bs",  # 后处理之后，啥也没剩 或者 最高分低于阈值的量，
            "type": "number",
        },
    ]
    bdp_manager = BDPManager(TABLE_NAME)
    unique_key = ["id"]  # 指定主键或者联合主键
    bdp_manager.create_table_if_not_exist(schema, unique_key)
    bdp_manager.commit()


def insert(fields, data):
    bdp_manager = BDPManager(TABLE_NAME)
    bdp_manager.insert_data(fields, data)


def main():
    today_zero, today_end = get_today_timestamp()
    ds = timestamp2datetime(0)
    print ds, type(ds)


def test_insert():
    fields = [
        "created_date",

        "news_all_input_qa",
        "news_all_output_qa",
        "news_no_info_qa",
        "news_filtered_by_preprocessing_qa",
        "news_empty_res_qa",
        "news_bad_res_qa",

        "news_all_input_bs",
        "news_all_output_bs",
        "news_no_info_bs",
        "news_filtered_by_preprocessing_bs",
        "news_empty_res_bs",
        "news_bad_res_bs"
    ]
    test_ds = timestamp2datetime(0)
    data = [[test_ds, 22968, 912, 8197, 1039, 185, 674,
             22960, 812, 8197, 1039, 185, 674]]
    bdp_manager = BDPManager(TABLE_NAME)
    bdp_manager.insert_data(fields, data)
    bdp_manager.commit()


def insert_kernel(fields, data):
    bdp_manager = BDPManager(TABLE_NAME)
    bdp_manager.insert_data(fields, data)
    bdp_manager.commit()
    print "data commited"




def insert_one_day():
    # 必须加id，否则默认为''，会覆盖之前的数据
    log_file_name1, ana_file_name1, bdp_file_name1, log_file_name2, ana_file_name2, bdp_file_name2, bdp_file_name = get_yesterday_log_filename()
    bdp_data = pickle_from_file(bdp_file_name)
    now = time.time()
    yesterday = now - 86400.0
    ds = timestamp2datetime(now)
    date_int = int(timestamp2date(yesterday))
    fields = ["created_date","id"]
    data0 = [ds,date_int]

    for field in bdp_data:
        cnt = bdp_data[field]
        print field, type(field)
        print cnt, type(cnt)
        fields.append(field)
        data0.append(cnt)
    print "fields", fields
    print "data0", data0

    data = [data0]
    insert_kernel(fields, data)


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == "test":
        test_insert()
    else:
        insert_one_day()
