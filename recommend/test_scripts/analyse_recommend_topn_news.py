# encoding=utf8


import os
import sys
import csv
from random import shuffle

from recommend.daily_scripts.utils import get_parti_solr_filename, get_parti_uid_filename
from general_utils.hbase_utils import cy_time_event_one_user_kernel2
from general_utils.text_utils import convert2gbk
from general_utils.solr_utils import nat_get_title, get_caled_user_topn_news
from general_utils.time_utils import get_yesterday_timestamp, timestamp2datetime
from recommend.manager.feed_data_helper import new_newsids_check
from rpc_services.user_profile_api import is_app_user


def main1():
    uids = []

    # 获取所有uid
    for i in (0, 1, 2, 3):
        uid_filename = get_parti_uid_filename(part=i, mode='news')
        with open(uid_filename, 'r') as f:
            ls = f.readlines()
            t_uids = [int(item.strip('\n')) for item in ls]
            uids.extend(t_uids)

    #
    output_filename = '20180312_user_event_and_recommend_news.csv'
    yesterday_begin, yesterday_end = get_yesterday_timestamp()
    yesterday_begin = int(yesterday_begin * 1000)
    yesterday_end = int(yesterday_end * 1000)

    #

    fo = open(output_filename, 'w')
    csvwriter = csv.writer(fo)
    first_line = ['uid', 'is_app_user', 'event_datetime', 'event_type', 'event_obj', 'recommended_news']
    csvwriter.writerow(first_line)

    all_cnt = 0
    good_cnt = 0

    shuffle(uids)
    for uid in uids[:1000]:
        all_cnt += 1
        is_app = is_app_user(uid)
        print '+' * 10, uid, '+' * 10
        user_action_list = cy_time_event_one_user_kernel2(uid, yesterday_begin, yesterday_end)
        recommended_news_ids = get_caled_user_topn_news(uid)
        recommended_news_ids = new_newsids_check(recommended_news_ids, 2)
        if recommended_news_ids:
            good_cnt += 1

        cnt = 0
        for i in range(max([len(user_action_list), len(recommended_news_ids)])):
            if cnt == 0:
                user_id = str(uid)


            else:
                user_id = ''
                is_app = ''
            try:
                event_datetime = timestamp2datetime(user_action_list[i][0] / 1000.0)
                event_type = user_action_list[i][2]
                event_obj = user_action_list[i][1]
                if event_type == 'vn':
                    title = nat_get_title('news_' + str(event_obj))
                    event_obj_str = str(event_obj) + '|' + title
                elif event_type == 'vt':
                    title = nat_get_title('topic_' + str(event_obj))
                    event_obj_str = str(event_obj) + '|' + title
                else:
                    event_obj_str = event_obj

            except:
                event_datetime = ''
                event_obj_str = ''
                event_type = ''

            try:
                recommended_news_id = recommended_news_ids[i]
                title = nat_get_title('news_' + str(recommended_news_id))
                recommend_str = str(recommended_news_id) + '|' + title

            except:
                recommend_str = ''

            line = convert2gbk([user_id, str(is_app), event_datetime, event_type, event_obj_str, recommend_str])
            csvwriter.writerow(line)

            cnt += 1

    line = ['all', 'good']
    csvwriter.writerow(line)
    csvwriter.writerow([str(all_cnt),str(good_cnt)])
    fo.close()


if __name__ == '__main__':
    main1()
