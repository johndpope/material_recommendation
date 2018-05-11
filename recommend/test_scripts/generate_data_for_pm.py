# encoding=utf8

import time
import sys
import csv
from collections import defaultdict

from general_utils.hbase_utils import cy_time_event_kernel_test
from recommend.manager.recommend_resource import Recommend_by_user_info
from general_utils.db_utils import get_medicaldb_handler, get_newsdb_handler
from general_utils.solr_utils import get_news_tags_from_solr, nat_get_title
from general_utils.text_utils import convert2gbk

from general_utils.time_utils import timestamp2datetime, ensure_second_timestamp


def main5(test_uid=None, now=None):
    if test_uid == "n":
        test_uid = None
    now = time.time()
    if not now:
        now = 1512379920.1
    else:
        now = float(ensure_second_timestamp(now))
    t10 = time.time()
    data_dict = cy_time_event_kernel_test(now - 12000.0, now, test_uid)
    t20 = time.time()

    print "len(data_dict)", len(data_dict)

    if not test_uid:
        fo = open("20171220_1_res.csv", "w")
    else:
        fo = open('test.csv', 'w')
    csvwriter = csv.writer(fo, dialect="excel")
    first_line = [u"uid", u"u_tags", u"special_population", u"trigger", u"trigger_info", u"trigger_time",
                  u"material_id", u"material_type", u"score", u"title", u"m_tags", u"only_topic", u"best_id",
                  u"best_score", u"time"]
    csvwriter.writerow(first_line)
    all_call_cnt = 0
    all_valid_res_cnt = 0
    exception_cnt = 0
    status_dict = defaultdict(int)

    total_time = []
    slow_case = []
    for uid in data_dict:

        all_call_cnt += 1

        user_info0 = data_dict[uid]
        try:
            # if True:
            t1 = time.time()
            res = Recommend_by_user_info(
                user_info0, uid, log_mark='testmain5', test=True)

            # return = {"user_info": None, "res": None, "topn_ids_scores": None, "only_topic": None,"status":"succeed"}

            t2 = time.time()
            print t2 - t1
            if t2 - t1 >= 3:
                break

            user_info = res['user_info']
            res1 = res['res']
            topn_ids_scores = res['topn_ids_scores']
            only_topic = res['only_topic']
            status = res['status']
            v_score_dict = res['v_score_dict']

            best_id, best_title, mtype = res1[0]

            this_time = t2 - t1
            if this_time >= 1.0:
                slow_case.append([uid, this_time])
            total_time.append(t2 - t1)
        except Exception, e:
            print e

            exception_cnt += 0
            continue
        status_dict[status] += 1

        ####################
        # if not only_topic:
        #     continue
        ####################

        if best_id == -1 or user_info is None:
            continue

        print '================='
        print uid

        texts = user_info["texts"]
        tags = user_info["tags"]
        special_population = user_info["special_population"]
        trigger = user_info["trigger"]
        timestamp = user_info['timestamp']
        best_score = v_score_dict[mtype + '_' + str(best_id)]
        # if trigger == "big_search":
        #     continue

        if trigger == 'big_search':
            trigger_info = "-".join(texts)
        elif trigger == "free_problem_create":
            problem_id, ask = get_medicaldb_handler().get_ask_by_timestamp(uid, timestamp)
            if not ask:
                ask = texts[0]
            trigger_info = '-'.join([str(problem_id), str(ask)])
        print "u tags", "-".join(tags), special_population
        print trigger_info, best_id, best_score, best_title

        for unique_id, score in topn_ids_scores:
            material_type, id = unique_id.split('_')
            if material_type == "news":
                title, _ = get_newsdb_handler().get_title_digest_by_nid(id)
                m_tags = get_news_tags_from_solr("news_" + str(id))
            elif material_type == "topic":
                title = get_medicaldb_handler().get_topic_title(id)
                m_tags = get_news_tags_from_solr("r_topic_" + str(id))

            rows = [str(uid), "-".join(tags), str(special_population),
                    trigger, trigger_info, str(timestamp),
                    str(id), material_type, str(score), title, "-".join(m_tags),
                    str(only_topic),
                    str(best_id), str(best_score), str(this_time)
                    ]
            rows = convert2gbk(rows)
            csvwriter.writerow(rows)
        all_valid_res_cnt += 1
    csvwriter.writerow([str(all_call_cnt), str(all_valid_res_cnt), str(exception_cnt)])

    for status_type in status_dict:
        csvwriter.writerow([status_type, str(status_dict[status_type])])

    csvwriter.writerow([str(sum(total_time) / len(total_time)), str(max(total_time)), str(min(total_time))])

    for uid, t in slow_case:
        csvwriter.writerow([str(uid), str(t)])
    fo.close()
    print "get data time", t20 - t10


def main6(test_uid=None):
    # test recommend_topics
    from recommend.manager.recommend_resource import Recommend_topics
    from recommend.manager.recommend_topic_data_helper import parse_user_info as parse_user_info2
    now = time.time()
    if test_uid == "n":
        test_uid = None
    data_dict = cy_time_event_kernel_test(now - 12000.0, now, test_uid)
    fo = open("20180102_rt.csv", "w")
    csvwriter = csv.writer(fo)
    first_line = ['uid', 'tags', 'sp', 'topicid', 'score', 't_title', 't_tags']
    csvwriter.writerow(first_line)
    times = {}
    for uid in data_dict.keys():
        t1 = time.time()
        topic_ids, user_info, score_dict = Recommend_topics(uid, 5, now, True)
        t2 = time.time()
        times[uid] = t2 - t1
        if not user_info:
            continue
        tags = user_info['tags']
        sp = user_info['special_population']
        for x in topic_ids:
            title = nat_get_title('topic_' + str(x))
            score = score_dict['topic_' + str(x)]
            t_tags = get_news_tags_from_solr("r_topic_" + str(x))
            row = [str(uid), '-'.join(tags), sp, str(x), str(score), title, '-'.join(t_tags)]
            row = convert2gbk(row)
            csvwriter.writerow(row)
    fo.close()

    s_times = sorted(times.iteritems(), key=lambda x: x[1], reverse=True)[:10]
    for x, y in s_times:
        print x, y


def main7(test_uid=None):
    # test recommend_list
    from recommend.manager.recommend_resource import Recommend_by_user_info
    if test_uid == "n":
        test_uid = None
    now = time.time()
    # now = 1520261533
    data_dict = cy_time_event_kernel_test(now - 3000, now, test_uid)
    if not test_uid:
        fo = open("20180306_1_rlr.csv", "w")
    else:
        fo = open('test.csv', 'w')
    csvwriter = csv.writer(fo, dialect="excel")
    first_line = [u"uid", u"u_tags", u"special_population", u"trigger", u"trigger_info", u"trigger_time",
                  u"material_id", u"material_type", u"score", u"title", u"m_tags", u"only_topic"]
    csvwriter.writerow(first_line)
    fail_cases = {
        'big_search': defaultdict(int),
        'free_problem_create': defaultdict(int)
    }

    all_uid_cnt = 0
    all_valid_res_cnt = 0
    qa_score = [[1.0 - i / 10.0, 0] for i in range(11)]
    bs_score = [[1.0 - i / 10.0, 0] for i in range(11)]
    trigger_cnt = {'qa': 0,
                   'bs': 0}
    cal_time = {}
    for uid in data_dict.keys():
        time.sleep(0.5)
        print '=' * 10, uid, '=' * 10

        user_info0 = data_dict[uid]
        t1 = time.time()
        res = Recommend_by_user_info(
            user_info0=user_info0,
            uid=uid,
            log_mark="test7",
            num=6,
            test=True
        )
        t2 = time.time()
        cal_time[uid] = t2 - t1

        user_info = res['user_info']
        res1 = res['res']
        topn_ids_scores = res['topn_ids_scores']
        only_topic = res['only_topic']
        status = res['status']
        v_score_dict = res['v_score_dict']
        if not user_info:
            continue
        all_uid_cnt += 1
        trigger = user_info["trigger"]
        if trigger == "big_search":
            trigger_cnt['bs'] += 1
        else:
            trigger_cnt['qa'] += 1

        if status != 'succeed':
            fail_cases[trigger][status] += 1
            continue

        texts = user_info["texts"]
        tags = user_info["tags"]
        special_population = user_info["special_population"]

        timestamp = user_info['timestamp']

        best_id, best_title, mtype = res1[0]
        best_score = v_score_dict[mtype + '_' + str(best_id)]

        if trigger == 'big_search':
            for i, item in enumerate(bs_score):
                if best_score >= item[0]:
                    bs_score[i][1] += 1
                    break
        else:
            for i, item in enumerate(qa_score):
                if best_score >= item[0]:
                    qa_score[i][1] += 1
                    break

        if trigger == 'big_search':
            trigger_info = "-".join(texts)
        elif trigger == "free_problem_create":
            problem_id, ask = get_medicaldb_handler().get_ask_by_timestamp(uid, timestamp)
            if not ask:
                ask = texts[0]
            trigger_info = '-'.join([str(problem_id), str(ask)])


            # [u"uid", u"u_tags", u"special_population", u"trigger", u"trigger_info", u"trigger_time",
            # u"material_id", u"material_type", u"score", u"title", u"m_tags", u"only_topic",

        for id, title, mtype in res1:
            prefix = 'news_' if mtype == 'news' else 'r_topic_'
            mtags = get_news_tags_from_solr(prefix + str(uid))
            rows = [str(uid), '-'.join(tags), special_population, trigger, trigger_info, str(timestamp),
                    str(id), mtype, v_score_dict[mtype + '_' + str(id)], title, '-'.join(mtags), str(only_topic)]

            rows = convert2gbk(rows)
            csvwriter.writerow(rows)
        if res1:
            all_valid_res_cnt += 1

    # fail_cases
    for trigger in fail_cases:
        for reason in fail_cases[trigger]:
            rows = [trigger, reason, str(fail_cases[trigger][reason])]
            rows = convert2gbk(rows)
            csvwriter.writerow(rows)
    # ana
    rows = ['all', str(all_uid_cnt), 'res_cnt', str(all_valid_res_cnt)]
    rows = convert2gbk(rows)
    csvwriter.writerow(rows)

    # score cut
    rows = ['bs score cut']
    csvwriter.writerow(rows)

    cum_cnt = 0
    for score, cnt in bs_score:
        cum_cnt += cnt
        true_recall = cum_cnt / float(trigger_cnt['bs'])
        rows = [str(score), str(cnt), str(true_recall)]
        csvwriter.writerow(rows)

    rows = ['qa score cut']
    csvwriter.writerow(rows)

    cum_cnt = 0
    for score, cnt in qa_score:
        cum_cnt += cnt
        true_recall = cum_cnt / float(trigger_cnt['bs'])
        rows = [str(score), str(cnt), str(true_recall)]
        csvwriter.writerow(rows)

    # cal time

    s_cal_time = sorted(cal_time.iteritems(), key=lambda x: x[1], reverse=True)
    for u, t in s_cal_time[:20]:
        csvwriter.writerow([str(u), str(t)])

    fo.close()


def main8(test_uid=None):
    # test cf
    from recommend.manager.recommend_resource import Recommend_by_user_info
    if test_uid == "n":
        test_uid = None
    now = time.time()
    # now = 1513780888
    data_dict = cy_time_event_kernel_test(now - 6000, now, test_uid)
    if not test_uid:
        fo = open("20171229_1_cfr.csv", "w")
    else:
        fo = open('test.csv', 'w')
    csvwriter = csv.writer(fo, dialect="excel")
    first_line = [u"uid", u"u_tags", u"special_population", u"trigger", u"trigger_info", u"trigger_time",
                  u"material_id", u"material_type", u"score", u"title", u"m_tags", u"only_topic"]
    csvwriter.writerow(first_line)
    fail_cases = {
        'big_search': defaultdict(int),
        'free_problem_create': defaultdict(int)
    }

    all_uid_cnt = 0
    all_valid_res_cnt = 0
    qa_score = [[1.0 - i / 10.0, 0] for i in range(11)]
    bs_score = [[1.0 - i / 10.0, 0] for i in range(11)]
    trigger_cnt = {'qa': 0,
                   'bs': 0}
    cal_time = {}
    for uid in data_dict.keys():
        time.sleep(0.5)
        print '=' * 10, uid, '=' * 10

        user_info0 = data_dict[uid]
        t1 = time.time()
        res = Recommend_by_user_info(
            user_info0=user_info0,
            uid=uid,
            log_mark="test8",
            num=6,
            test=True
        )
        t2 = time.time()
        cal_time[uid] = t2 - t1

        user_info = res['user_info']
        res1 = res['res']
        topn_ids_scores = res['topn_ids_scores']
        only_topic = res['only_topic']
        status = res['status']
        v_score_dict = res['v_score_dict']
        if not user_info:
            continue
        all_uid_cnt += 1
        trigger = user_info["trigger"]
        if trigger == "big_search":
            trigger_cnt['bs'] += 1
        else:
            trigger_cnt['qa'] += 1

        if status != 'succeed':
            fail_cases[trigger][status] += 1
            continue

        texts = user_info["texts"]
        tags = user_info["tags"]
        special_population = user_info["special_population"]

        timestamp = user_info['timestamp']

        best_id, best_title, mtype = res1[0]
        best_score = v_score_dict[mtype + '_' + str(best_id)]

        if trigger == 'big_search':
            for i, item in enumerate(bs_score):
                if best_score >= item[0]:
                    bs_score[i][1] += 1
                    break
        else:
            for i, item in enumerate(qa_score):
                if best_score >= item[0]:
                    qa_score[i][1] += 1
                    break

        if trigger == 'big_search':
            trigger_info = "-".join(texts)
        elif trigger == "free_problem_create":
            problem_id, ask = get_medicaldb_handler().get_ask_by_timestamp(uid, timestamp)
            if not ask:
                ask = texts[0]
            trigger_info = '-'.join([str(problem_id), str(ask)])


            # [u"uid", u"u_tags", u"special_population", u"trigger", u"trigger_info", u"trigger_time",
            # u"material_id", u"material_type", u"score", u"title", u"m_tags", u"only_topic",

        for id, title, mtype in res1:
            prefix = 'news_' if mtype == 'news' else 'r_topic_'
            mtags = get_news_tags_from_solr(prefix + str(uid))
            rows = [str(uid), '-'.join(tags), special_population, trigger, trigger_info, str(timestamp),
                    str(id), mtype, v_score_dict[mtype + '_' + str(id)], title, '-'.join(mtags), str(only_topic)]

            rows = convert2gbk(rows)
            csvwriter.writerow(rows)
        if res1:
            all_valid_res_cnt += 1

    # fail_cases
    for trigger in fail_cases:
        for reason in fail_cases[trigger]:
            rows = [trigger, reason, str(fail_cases[trigger][reason])]
            rows = convert2gbk(rows)
            csvwriter.writerow(rows)
    # ana
    rows = ['all', str(all_uid_cnt), 'res_cnt', str(all_valid_res_cnt)]
    rows = convert2gbk(rows)
    csvwriter.writerow(rows)

    # score cut
    rows = ['bs score cut']
    csvwriter.writerow(rows)

    cum_cnt = 0
    for score, cnt in bs_score:
        cum_cnt += cnt
        true_recall = cum_cnt / float(trigger_cnt['bs'])
        rows = [str(score), str(cnt), str(true_recall)]
        csvwriter.writerow(rows)

    rows = ['qa score cut']
    csvwriter.writerow(rows)

    cum_cnt = 0
    for score, cnt in qa_score:
        cum_cnt += cnt
        true_recall = cum_cnt / float(trigger_cnt['bs'])
        rows = [str(score), str(cnt), str(true_recall)]
        csvwriter.writerow(rows)

    # cal time

    s_cal_time = sorted(cal_time.iteritems(), key=lambda x: x[1], reverse=True)
    for u, t in s_cal_time[:20]:
        csvwriter.writerow([str(u), str(t)])

    fo.close()



def main9(test_uid=None):
    # test recommend tags
    from recommend.manager.recommend_resource import Recommend_tags
    from recommend.manager.recommend_tags_data_helper import get_user_last_query
    if test_uid == "n":
        test_uid = None
    now = time.time()
    # now = 1513780888
    data_dict = cy_time_event_kernel_test(now - 2000, now, test_uid)
    if not test_uid:
        fo = open("20180102_1_rtr.csv", "w")
    else:
        fo = open('test.csv', 'w')

    first_line = ['uid','last_query','r_tags','r_plan']
    csvwriter = csv.writer(fo)
    csvwriter.writerow(first_line)
    total_t = {}
    for uid in data_dict.keys():
        t1 = time.time()
        res  = Recommend_tags(uid)
        t2 = time.time()
        total_t[uid] = t2 - t1
        #{'words': tags, 'plan': plans}
        words = res['words']
        plan = res['plan']
        last_query = get_user_last_query(uid)
        row = [str(uid),last_query,'-'.join(words),'-'.join([item['name'] for item in plan])]
        row = convert2gbk(row)
        csvwriter.writerow(row)

    s_total_t = sorted(total_t.iteritems(),key=lambda x:x[1],reverse=True)
    for uid,t in s_total_t[:10]:
        csvwriter.writerow([str(uid),str(t)])






if __name__ == '__main__':
    test_uid = sys.argv[1]
    ts = sys.argv[2]
    # main5(test_uid, ts)
    main7(test_uid)
