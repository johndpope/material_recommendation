# encoding=utf8

import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
import sys
import json

from general_utils.time_utils import get_yesterday_date
from recommend.manager.recommend_topic_data_helper import get_all_yesterday_user_id
from recommend.manager.recommend_resource import Recommend_topics
from recommend.daily_scripts.utils import get_parti_uid_filename, get_parti_solr_filename

# from recommend.app_config import USER_TOPN_TOPIC_DATADIR

cut_num = 4
yesterday_date = get_yesterday_date()


def save_uids(test=False):
    '''
    获取前一天活跃用户id，分成4份存在文件里
    :return:
    '''

    all_valid_uids = get_all_yesterday_user_id(test=test)
    all_valid_uids = [str(x) for x in all_valid_uids]
    l = len(all_valid_uids)

    cut_index = [x * l / cut_num for x in range(cut_num + 1)]
    for i in range(cut_num):
        file_name = get_parti_uid_filename(i, 'topic')
        with open(file_name, 'w') as f:
            f.write('\n'.join(all_valid_uids[cut_index[i]:cut_index[i + 1]]))


def cal_uids_part(part):
    '''
    part:0,1,2,3
    :param part:
    :return:
    '''
    if int(part) not in range(cut_num):
        return

    filename = get_parti_uid_filename(part, 'topic')
    with open(filename, 'r') as f:
        uids = f.readlines()
    uids = [x.strip('\n') for x in uids]
    cal_yesterday_user_topn_topics(uids, get_parti_solr_filename(part, 'topic'))


def cal_yesterday_user_topn_topics(uids, output_filename):
    fo = open(output_filename, 'w')

    for uid in uids:
        topic_ids = Recommend_topics(uid, 5)
        key = yesterday_date + '|' + str(uid)
        line = json.dumps({'key': key, 'topics': topic_ids}) + '\n'
        fo.write(line)

    fo.close()


if __name__ == '__main__':
    mode = sys.argv[1]

    if mode == 'get_uid':
        save_uids()
    elif mode == 'cal':
        part = sys.argv[2]
        cal_uids_part(part)
