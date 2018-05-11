# encoding=utf8

import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
import sys
import json
import time

from general_utils.time_utils import get_yesterday_date
from recommend.manager.feed_data_helper import get_all_yesterday_user_id
from recommend.manager.recommend_resource import recommend_news_kernel
from add_data_to_solr.manager.user_topn_news import add_a_part
from recommend.daily_scripts.utils import get_parti_solr_filename, get_parti_uid_filename

cut_num = 4
yesterday_date = get_yesterday_date()


def save_uids(test=False):
    '''
    获取前一天活跃用户id，分成4份存在文件里(每份5000多个uid）
    :return:
    '''

    all_valid_uids = get_all_yesterday_user_id(test=test)
    all_valid_uids = [str(x) for x in all_valid_uids]
    l = len(all_valid_uids)

    cut_index = [x * l / cut_num for x in range(cut_num + 1)]
    for i in range(cut_num):
        file_name = get_parti_uid_filename(i, 'news')
        with open(file_name, 'w') as f:
            f.write('\n'.join(all_valid_uids[cut_index[i]:cut_index[i + 1]]))


def cal_uids_part(part, test=False):
    '''
    part:0,1,2,3
    :param part:
    :return:
    '''
    if int(part) not in range(cut_num):
        return

    filename = get_parti_uid_filename(part, 'news')
    with open(filename, 'r') as f:
        uids = f.readlines()
    uids = [x.strip('\n') for x in uids]
    if test:
        uids = uids[:10]
    cal_yesterday_user_topn_news(uids, get_parti_solr_filename(part, 'news'))


def cal_and_add_part(part, test=False):
    cal_uids_part(part, test)
    add_a_part(part)


def cal_yesterday_user_topn_news(uids, output_filename):
    fo = open(output_filename, 'w')

    for uid in uids:
        last_event_time = time.time()
        res = recommend_news_kernel(uid, test=False, num=20)
        news_ids = res['ids']
        # news_ids = Recommend_news(uid, 20, solr_first=False)  # 一定要设置solr_first=False

        line = json.dumps({'uid': uid, 'ids': news_ids, 'last_event_time': last_event_time}) + '\n'
        fo.write(line)

    fo.close()


if __name__ == '__main__':
    mode = sys.argv[1]

    if mode == 'get_uid':
        save_uids()
    elif mode == 'cal':
        part = sys.argv[2]
        test = (sys.argv[3] == 'test')
        cal_uids_part(part, test)
    elif mode == 'cal_and_add':
        part = sys.argv[2]
        test = (sys.argv[3] == 'test')
        cal_and_add_part(part, test)
