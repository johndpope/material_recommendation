# encoding=utf8

import os
import sys
import csv
import time
import random

from general_utils.time_utils import ensure_second_timestamp, timestamp2datetime, datetime_str2timestamp
from general_utils.text_utils import convert2gbk
from rpc_services.user_profile_api import get_username, is_app_user


def get_one_day_uid_from_file(file_name):
    uids = set()
    with open(file_name, 'r') as f:
        for l in f:
            uid = l.split('|')[0]
            uids.add(int(uid))
    return list(uids)


def g1():
    '''
    查看不使用热卖tag扩充的覆盖率，和使用热卖tag扩充的覆盖率

    分子，能匹配上热卖tag的，分母，一天内有活动用户（cy_event
    '''

    from general_utils.hbase_utils import get_user_query, get_user_query2
    from general_utils.solr_utils import get_last_login_uids
    from recommend.manager.recommend_tags_data_helper import get_relation_plan3
    from general_utils.db_utils import get_db_data_local_handler
    from general_utils.hbase_utils import get_sp_duration_active_userid

    from general_utils.time_utils import timestamp2datetime, ensure_second_timestamp
    # 用户采样时间窗
    # 用户采样命中率

    end_ds0 = '2018-01-21 23:59:40'
    end0 = datetime_str2timestamp(end_ds0)
    begin0 = end0 - 86400 * 1

    # 每个选中用户的数据采集时间窗
    end_ds = '2018-01-22 23:59:40'
    end = datetime_str2timestamp(end_ds)
    begin = end - 86400 * 180.0  # 半年

    # 最后登录时间在2018-01-21 23:59:40前一周的用户
    # test_uids = get_last_login_uids(begin0, end0)
    # test_uids = get_sp_duration_active_userid(begin0,end0)
    test_uids = get_one_day_uid_from_file('log_event_20180122')
    print "test_uids num", len(test_uids)

    # 打乱顺序，取1000个样本
    random.shuffle(test_uids)
    selected_uids = test_uids[:3000]

    all_good_cnt = 0
    all_cnt = 0
    app_cnt = 0
    good_app_cnt = 0

    text_empty_cnt = 0
    fo = open('180129_rp_1.csv', 'w')
    csvwriter = csv.writer(fo)
    first_line = ['uid', 'username', 'is_app', 'last_info_time', 'use_tags', 'systag_ids', 'tag_names', 't',
                  'is_tangsai']
    csvwriter.writerow(first_line)
    # status_dict = {
    #     1: "qa and query",
    #     2: "view actions",
    #     3: "search_doctor clinic_no",
    #     0: ""
    # }

    total_time = {}
    for uid in selected_uids:
        print '==============uid=%s=======================' % uid
        username = get_username(uid)
        is_app = is_app_user(uid)

        all_cnt += 1
        if is_app:
            app_cnt += 1

        t1 = time.time()
        res = get_relation_plan3(uid, test=True)
        t2 = time.time()
        t = t2 - t1
        total_time[uid] = t
        status = res['status']
        is_tangsai = False
        if status:
            all_good_cnt += 1
            if is_app:
                good_app_cnt += 1
            systag_ids = res['ids']
            if 96 in systag_ids:
                is_tangsai = True
            tagnames = [get_db_data_local_handler().get_systagid_name(id) for id in systag_ids]
            if status in (1, 2, 4):
                info0 = res['systag_id_dict']
                record_info = '~'.join(info0.keys())
            elif status == 3:
                info0 = res['clinic_no']
                record_info = '~'.join(info0)
            last_ts = res['last_ts']
            last_info_time = timestamp2datetime(ensure_second_timestamp(last_ts))


        else:
            systag_ids = []
            tagnames = []
            record_info = ''
            last_info_time = ''

        systag_ids_str = '~'.join([str(x) for x in systag_ids])
        tagnames_str = '~'.join(tagnames)

        line = convert2gbk(
            [str(uid), username, str(is_app), last_info_time, record_info, systag_ids_str, tagnames_str, str(t),
             str(is_tangsai)])
        csvwriter.writerow(line)

    line = [str(all_cnt), str(all_good_cnt), str(app_cnt), str(good_app_cnt)]
    csvwriter.writerow(line)
    s_total_time = sorted(total_time.iteritems(), key=lambda x: x[1], reverse=True)
    times = total_time.values()
    line = [str(min(times)), str(max(times)), str(sum(times) / len(times))]
    csvwriter.writerow(line)
    for uid, t in s_total_time[:10]:
        line = [str(uid), str(t)]
        csvwriter.writerow(line)

    fo.close()

    print str(max(times))
    print all_good_cnt


def g2():
    # test recommend_news
    '''

    :return:
    '''
    from recommend.manager.feed_data_helper import recommend_news_kernel
    from general_utils.solr_utils import nat_get_title

    test_uids = get_one_day_uid_from_file('log_event_20180222')
    print "test_uids num", len(test_uids)

    # 打乱顺序，取1000个样本
    random.shuffle(test_uids)
    selected_uids = test_uids[:1000]

    fo = open('20180321_rn_1.csv', 'w')
    csvwriter = csv.writer(fo)
    first_line = ['uid', 'username', 'utags', 'user_bs', 'user_qa', 'user_look_title', 'news_id', 'title', 'score']
    csvwriter.writerow(first_line)
    total_time = {}

    cnt_all = 0
    cnt_good = 0

    for uid in selected_uids:
        print '==============uid=%s=======================' % uid
        username = get_username(uid)
        is_app = is_app_user(uid)
        if not is_app:
            continue
        cnt_all += 1
        t1 = time.time()
        recommend_res = recommend_news_kernel(uid, True)
        t2 = time.time()
        total_time[uid] = t2 - t1
        parsed_user_info = recommend_res['parsed_user_info']
        utags = parsed_user_info['weight_dict'].keys()
        user_info_list = recommend_res['user_info_list']

        bs_text_list = []
        qa_text_list = []
        view_news_title_list = []
        view_topic_title_list = []

        for ts, obj, action_type in user_info_list:
            if action_type in ('bs', 'sd'):
                bs_text_list.append(obj)
            elif action_type == 'qa':
                qa_text_list.append(obj)
            elif action_type == 'vt':
                title = nat_get_title('topic_' + str(obj))
                view_topic_title_list.append(title)
            elif action_type == 'vn':
                title = nat_get_title('news_' + str(obj))
                view_news_title_list.append(title)

        user_bs = '~'.join([str(item) for item in bs_text_list])
        user_qa = '~'.join([str(item) for item in qa_text_list])
        user_look_title = '~'.join([str(item) for item in view_news_title_list + view_topic_title_list])

        title_dict = recommend_res['title_dict']
        ids_list = recommend_res['ids']
        score_dict = recommend_res['v_score_dict']
        ids = [['%s-news_' % i + str(x) for x in ids] for [i, ids] in enumerate(ids_list)]
        ids1 =[]
        for x in ids:
            ids1.extend(x)
        ids = ids1
        tcnt = 0

        if ids:
            cnt_good += 1
        for id in ids:
            id0 = id.split('-')[1]
            title = title_dict[id0]
            score = score_dict[id0]
            if tcnt == 0:
                line = convert2gbk([str(uid), username, '~'.join(utags),
                                    user_bs, user_qa, user_look_title,
                                    str(id), title, score])
            else:
                line = convert2gbk([' ', ' ', '~'.join(utags),
                                    user_bs, user_qa, user_look_title,
                                    str(id), title, score])
            csvwriter.writerow(line)
            tcnt += 1

    min_t = min(total_time.values())
    max_t = max(total_time.values())
    mean_t = sum(total_time.values()) / len(total_time)

    line = ['min', 'max', 'mean']
    csvwriter.writerow(line)
    line = [str(min_t), str(max_t), str(mean_t)]
    csvwriter.writerow(line)

    sorted_total_time = sorted(total_time.iteritems(), key=lambda x: x[1], reverse=True)
    for uid, t in sorted_total_time[:10]:
        line = [str(uid), str(t)]
        csvwriter.writerow(line)

    line = ['all_app_user_num', 'good_add_user_num']
    csvwriter.writerow(line)
    line = [str(cnt_all), str(cnt_good)]
    csvwriter.writerow(line)

    fo.close()


if __name__ == '__main__':
    g2()
