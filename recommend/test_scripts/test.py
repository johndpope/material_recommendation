# encoding=utf8

import sys
import csv
import json
from collections import defaultdict

from general_utils.time_utils import ensure_second_timestamp, timestamp2datetime
from recommend.test_scripts.test_tools import *
from general_utils.text_utils import convert2gbk
from general_utils.file_utils import pickle_from_file, pickle_to_file
from general_utils.word2vec_utils import vecs_similarity_Hausdorff, vecs_similarity_weighted_Hausdorff, \
    outlier_rejection
from rpc_services.word2vec_api import get_vecs2, get_vec_dict_norm_ndarray_redis


def test1():
    uid = sys.argv[2]
    end = sys.argv[3]
    interval = sys.argv[4]
    end = ensure_second_timestamp(end)
    begin = end - int(interval) * 61.0
    end += int(interval) * 61.0
    print "begin", timestamp2datetime(begin)
    print "end", timestamp2datetime(end)
    user_time_event(uid, begin, end)


def test3():
    from collections import defaultdict
    # 2017-11-12 17:05:33,289 INFO recommend_resource.Recommend Line:52  failed in recommend==user_info is None ===uid=128243057===========
    filename = sys.argv[2]
    lookback_list = [5, 10, 15, 20, 30, 60, 120]
    res = defaultdict(list)
    trigger_count = {"big_search": 0, "free_problem_create": 0}
    with open(filename, 'r') as f:
        for l in f:
            dt = l.split(',')[0]
            uid = l.split("==uid=")[1].split('=')[0]
            end = ensure_second_timestamp(dt)
            begin_list = [end - x * 61.0 for x in lookback_list]

            end += 5.0
            index, trigger = user_time_event2(uid, end, begin_list)
            if trigger:
                trigger_count[trigger] += 1
            # print uid, index, trigger
            res[index].append([uid, dt, trigger])
    lookback_list.append(0)
    print res[0]
    for index in res.keys():
        print lookback_list[index], "分钟内可以召回", len(res[index])

    for trigger in trigger_count:
        print trigger, trigger_count[trigger]


def test2():
    # test vector sets similarities
    s1 = [u"腹痛", u"拉肚子", u"恶心"]
    s2 = [u"肚痛", u"拉稀", u"反胃"]
    s3 = [u"肚痛", u"拉肚子", u"反胃"]
    s4 = [u"肚痛", u"拉稀"]
    s5 = [u"肚痛", u"拉肚子"]
    s8 = [u"痘痘", u"脚气", u"肺气肿", u"ct"]
    s6 = s2 + [u"干呕"]
    s7 = s2 + s8
    s9 = s8 + [u"内分泌失调", u"子宫肌瘤"]
    s10 = [u"腹痛"]
    s11 = [u"肚痛"]
    s12 = [u"脚气"]

    s13 = [u"烫伤", u"水肿"]
    s14 = [u"烫伤"]
    s15 = [u"水肿"]

    s16 = s1 + [u'痘痘', u'青春痘']
    s17 = s2 + [u'痤疮', u'粉刺', u'豆豆', u'青春豆']
    s18 = s2 + [u'肛门', u'会阴', u'屁眼'] * 3

    print "一毛一样组"
    print "|||".join(s1), "vs", "|||".join(s1), test2_kernel(s1, s1)
    print "|||".join(s4), "vs", "|||".join(s4), test2_kernel(s4, s4)
    print "|||".join(s9), "vs", "|||".join(s9), test2_kernel(s9, s9)
    print "|||".join(s10), "vs", "|||".join(s10), test2_kernel(s10, s10)

    print "相似组"

    print "|||".join(s1), "vs", "|||".join(s2), test2_kernel(s1, s2)
    print "|||".join(s1), "vs", "|||".join(s3), test2_kernel(s1, s3)
    print "|||".join(s2), "vs", "|||".join(s3), test2_kernel(s2, s3)
    print "|||".join(s1), "vs", "|||".join(s6), test2_kernel(s1, s6)
    print "|||".join(s10), "vs", "|||".join(s11), test2_kernel(s10, s11)

    print "半相似组"

    print "|||".join(s1), "vs", "|||".join(s4), test2_kernel(s1, s4)
    print "|||".join(s1), "vs", "|||".join(s5), test2_kernel(s1, s5)
    print "|||".join(s2), "vs", "|||".join(s4), test2_kernel(s2, s4)
    print "|||".join(s2), "vs", "|||".join(s5), test2_kernel(s2, s5)
    print "|||".join(s1), "vs", "|||".join(s7), test2_kernel(s1, s7)
    print "|||".join(s2), "vs", "|||".join(s7), test2_kernel(s2, s7)
    print "|||".join(s1), "vs", "|||".join(s10), test2_kernel(s1, s10)
    print "|||".join(s1), "vs", "|||".join(s11), test2_kernel(s1, s11)

    print "不相似组"
    print "|||".join(s1), "vs", "|||".join(s12), test2_kernel(s1, s12)
    print "|||".join(s1), "vs", "|||".join(s9), test2_kernel(s1, s9)
    print "|||".join(s10), "vs", "|||".join(s12), test2_kernel(s10, s12)

    print "其他"
    print "|||".join(s13), "vs", "|||".join(s14), test2_kernel(s13, s14)
    print "|||".join(s13), "vs", "|||".join(s15), test2_kernel(s13, s15)
    print '|||'.join(s16), "vs", "|||".join(s17), test2_kernel(s16, s17)
    print '|||'.join(s16), "vs", "|||".join(s18), test2_kernel(s16, s18)
    print '|||'.join(s16), "vs", "|||".join(s2), test2_kernel(s16, s2)


def test2_kernel(words1, words2):
    vecs1 = get_vecs2(words1)
    vecs2 = get_vecs2(words2)
    weights = [1.0] * 20
    return vecs_similarity_weighted_Hausdorff(vecs1, weights, vecs2, weights)


def test4():
    from general_utils.db_utils import get_medical_entity_handler
    sql = 'select id,name,frequency from medicaldb_newdiseases;'
    o = get_medical_entity_handler(False).do_one(sql)
    fo = open("diseases_frequency.csv", "w")
    csvwriter = csv.writer(fo, dialect='excel')
    first = ["id", "name", "freq"]
    csvwriter.writerow(first)
    for item in o:
        id = item[0]
        name = item[1]
        freq = item[2]
        rows = [id, name, freq]
        rows = convert2gbk(rows)
        csvwriter.writerow(rows)
    fo.close()


def test5():
    from rpc_services.medical_service_utils import get_entities
    from rpc_services.search_api import more_topic
    from general_utils.db_utils import get_medicaldb_handler

    sql = "select ask from ask_problem order by id desc limit 1000;"
    o = get_medicaldb_handler().do_one(sql)
    yes = 0
    all = 0
    for item in o:
        print "---=-=-==-==-==--=--===-=="
        text = item[0]
        tags = " ".join(get_entities(text))
        print "text", text
        if not text:
            continue

        o = more_topic(text)
        o = json.loads(o)["result"]
        for item in o:
            print item['title']

        print len(o)
        print "=================="
        print "tags", tags
        if not tags:
            continue
        o = more_topic(tags)
        o = json.loads(o)["result"]
        for item in o:
            print item['title']
        cnt = 0
        print len(o)


def test6():
    # test recommmed
    from recommend.manager.recommend_resource import Recommend
    from general_utils.time_utils import datetime_str2timestamp
    uid = 126166712
    ts = datetime_str2timestamp("2017-11-20 10:24:57")
    o = Recommend(uid=uid, lookback=5 * 61.0, end=ts)
    print o


def test7():
    from general_utils.file_utils import pickle_from_file
    filename = sys.argv[2]
    solr_file = "data_dir/topic_data/" + filename
    solr_docs = pickle_from_file(solr_file)
    from add_data_to_solr.manager.add import add as add_all
    from add_data_to_solr.cy_solr_local.solr_base import SolrHelper
    solr = SolrHelper("online").get_solr("topic_tpl")
    add_all(solr_docs, solr)


def test8():
    from general_utils.file_utils import pickle_from_file, pickle_to_file
    from general_utils.db_utils import get_medicaldb_handler
    d = "data_dir/topic_data/"

    s1 = pickle_from_file(d + 'all_doc_small')
    s2 = pickle_from_file(d + 'all_doc_small_2')
    s3 = pickle_from_file(d + 'all_doc_small_3')
    b1 = pickle_from_file(d + 'all_doc_big')
    b2 = pickle_from_file(d + 'all_doc_big_2')
    b3 = pickle_from_file(d + 'all_doc_big_3')
    s = s1 + s2 + s3  # 用不到
    b = b1 + b2 + b3

    fo_name = "data_dir/topic_score.pickle"

    res = {}
    open(fo_name, "w").close()
    for item in b:
        id = int(item['id'].split('_')[-1])
        score = item['tid'] / 10.0
        res[id] = score
    pickle_to_file(res, fo_name)


def test9():
    from general_utils.db_utils import get_medicaldb_handler
    from add_data_to_solr.manager.add_utils import topic_info, doctor_info
    fo = open("topic_score.csv", "w")
    csvwriter = csv.writer(fo, dialect='excel')
    first_line = [u'topic id', u'score', u'topic title', u'content len', u'image num', u'is original',
                  u'doctor id', u'职称', u'医院级别', u'1科室', u"2科室", u'城市', u'hospital_name']
    first_line = convert2gbk(first_line)
    csvwriter.writerow(first_line)
    d = "data_dir/topic_data/"
    b2 = pickle_from_file(d + 'all_doc_big_2')
    for item in b2:
        id = int(item['id'].split('_')[-1])
        score = item['tid'] / 10.0
        title = item['title']
        content_len = item['content_len']
        sql = 'select doctor_id from api_doctortopic where id=%s;' % id
        o = get_medicaldb_handler().do_one(sql)
        doctor_id = o[0][0]
        ti = topic_info(id)
        di = doctor_info(doctor_id)
        image_num = ti['image_num']
        is_original = ti['is_original']
        d_title = di['title']
        h_level = di['hospital_level']
        h_name = di['hospital_name']
        clinic_no = di['first_class_clinic_no']
        s_clinic_no = di['second_class_clinic_no']
        city = di['city']
        rows = [str(id), str(score), title, str(content_len), str(image_num),
                str(is_original), doctor_id, d_title, h_level, clinic_no, s_clinic_no, city, h_name]
        rows = convert2gbk(rows)
        csvwriter.writerow(rows)
    fo.close()


def test10():
    from rpc_services.word2vec_api import get_similar
    from rpc_services.medical_service_api import tokenizer_default

    # 寻找相似词
    # id  query  分词结果  实体词分类  疾病词1 疾病词2 疾病词3  症状词1 症状词2 症状词3 药品词1 药品词2  药品词3
    # input_file = "/Users/satoshi/Documents/work file/query_result_o1.csv"
    input_file = sys.argv[2]
    endict = pickle_from_file("/home/classify/workspace/medical_data/data_dir/medical_word_detail.pickle")
    first_line = [u"id", u"query", u"words", u"cates",
                  u"disease",
                  u"symptom",
                  u"drug", ]
    fo = open("query_similar_words.csv", "w")
    csvwriter = csv.writer(fo, dialect='excel')
    csvwriter.writerow(first_line)

    with open(input_file, 'r') as f:
        for l in f:
            ll = l.strip('\n').split(',')
            print l
            print ll
            id, text = ll[0], ll[1]
            text = text.decode('gbk', 'ignore')
            similar_word_score_dict = {}
            seged = []
            cates = []
            tokens = tokenizer_default([text])["tokens"][0]

            for item in tokens:
                if u"neg_ne" in item:
                    continue
                if "cate" not in item:
                    continue
                word = item['token']
                if word in seged:
                    continue
                seged.append(word)
                cates.append(item['cate'])

            for x in seged:
                x_s = get_similar(x, 100)
                if not x_s:
                    continue
                for w, s in x_s:
                    if w not in similar_word_score_dict:
                        similar_word_score_dict[w] = s
                    elif s > similar_word_score_dict[w]:
                        similar_word_score_dict[w] = s

            dis = []
            sym = []
            drug = []
            s_similar_word_score = sorted(similar_word_score_dict.iteritems(), key=lambda x: x[1], reverse=True)
            for w, s in s_similar_word_score:
                if w not in endict:
                    continue
                cate = endict[w]['cate']
                if cate == "SYMPTOM_DESC" and len(sym) < 3:
                    sym.append(w)
                if cate == "DISEASE_DESC" and len(dis) < 3:
                    dis.append(w)
                if cate == "DRUG_DESC" and len(drug) < 3:
                    drug.append(w)
            row = [id, text, u"|||".join(seged), u"|||".join(cates), u"|||".join(dis), u"|||".join(sym),
                   u"|||".join(drug)]
            row = convert2gbk(row)
            csvwriter.writerow(row)
    fo.close()


def test11():
    filename = "/home/classify/workspace/medical_data/data_dir/medical_word_detail.pickle"
    endict = pickle_from_file(filename)
    fo = open("all_medical_words.csv", "w")
    csvwriter = csv.writer(fo, dialect='excel')

    for w in endict:
        id = endict[w]['id']
        cate = endict[w]['cate']

        row = [str(id), w, cate]
        row = convert2gbk(row)
        csvwriter.writerow(row)
    fo.close()


def test12():
    from general_utils.db_utils import get_db_data_local_handler

    w = sys.argv[2]
    o = get_db_data_local_handler().get_entity_cate(w)
    print o


def test13():
    from recommend.manager.recommend_resource import Recommend
    uid = 10605542
    pid = 615163971
    o = Recommend(uid=uid, lookback=5 * 61.0, end=1513240200.0, pid=pid)
    print o


def test14():
    from add_data_to_solr.manager.add_utils import topic_info
    from general_utils.db_utils import get_medicaldb_handler
    sql = 'select id from api_doctortopic where is_deleted=0 and title <> "";'
    o = get_medicaldb_handler().do_one(sql)
    fo = open('topic_content_len.csv', 'w')
    csvwriter = csv.writer(fo)
    first_line = ['topic id', 'doctor id', 'content length']
    csvwriter.writerow(first_line)

    for item in o:
        id = int(item[0])
        info_of_topic = topic_info(id)
        doctor_id = info_of_topic['doctor_id']
        content_len = info_of_topic['content_len']
        csvwriter.writerow([str(id), str(doctor_id), str(content_len)])
    fo.close()


def test15():
    '''
    分析recommend_list的log,然后统计出推出结果的uid0和没推出结果的uid1，以及他们三天内是否有点击news和topic的行为
    以验证相似文章的方法可以增加多少召回
    :return:
    '''

    from general_utils.hbase_utils import get_user_recent_views
    from general_utils.time_utils import datetime_str2timestamp

    file_name = sys.argv[2]
    all_uids = []
    succeed_uids = []
    failed_uids = {}
    uis_ts = {}

    all_good_news = set()
    with open('cy_event_view_news.json.knn_output', 'r') as f:
        for l in f:
            l = l.strip('\n')
            l = json.loads(l)
            news_id = l['id']
            top = l['top']
            if top:
                all_good_news.add(news_id)

    all_good_topic = set()
    with open('cy_event_view_topic.json.knn_output', 'r') as f:
        for l in f:
            l = l.strip('\n')
            l = json.loads(l)
            topic_id = l['id']
            top = l['top']
            if top:
                all_good_topic.add(topic_id)

    with open(file_name, 'r') as f:
        for l in f:
            # print l
            try:
                if "recommend_topn===============start" in l:
                    uid = int(l.split("uid=")[1].split('=')[0])
                    all_uids.append(uid)
                    dt = l.split(',')[0]
                    ts = datetime_str2timestamp(dt)
                    uis_ts[uid] = ts
                if "recommend_topn==succeed" in l:
                    uid = int(l.split("uid=")[1].split('=')[0])
                    succeed_uids.append(uid)
                if "recommend_topn==failed" in l:
                    reason = l.split('failed in recommend==')[1].split('=')[0]
                    uid = int(l.split("uid=")[1].split('=')[0])
                    failed_uids[uid] = reason
            except:
                pass
    print len(all_uids)
    print len(succeed_uids)
    print len(failed_uids)
    print len(succeed_uids) + len(failed_uids)

    reason_cnt = defaultdict(int)
    for uid in failed_uids:
        reason_cnt[failed_uids[uid]] += 1
    for x in reason_cnt:
        print x, reason_cnt[x]

    fha = []
    fha1 = []
    for uid in failed_uids:
        ts = uis_ts[uid]
        actions = get_user_recent_views(uid, now=ts, lookback=7 * 24 * 86400.0)

        if actions:
            fha.append(uid)

        # actions list of [ts,action_type,id]
        news_ids = set([item[2] for item in actions if item[1] == 'view_news'])
        topic_ids = set([item[2] for item in actions if item[1] == 'view_topic'])
        if (news_ids & all_good_news) or (topic_ids & all_good_topic):
            fha1.append(uid)

    print len(fha)
    print 'fha1', len(fha1)

    sha = []
    for uid in succeed_uids:
        ts = uis_ts[uid]
        actions = get_user_recent_views(uid, now=ts, lookback=7 * 24 * 86400.0)
        if actions:
            sha.append(uid)
    print len(sha)


def test16():
    fn = 'cy_event_rowkey.txt'
    types = defaultdict(int)
    with open(fn, 'r') as f:
        for l in f:
            action_type, ts, uid = l.split('|')
            types[action_type] += 1
    for x in types:
        print x, types[x]


def test17():
    from general_utils.solr_utils import nat_get_title
    fin = sys.argv[2]
    mtype = 'topic' if 'topic' in fin else 'news'
    fon = mtype + '_nearest_top10.csv'
    fo = open(fon, 'w')
    csvwriter = csv.writer(fo)
    first_line = ['id', 'title', 'top_id', 'top_title', 'score']
    csvwriter.writerow(first_line)
    cnt = 0
    with open(fin, 'r') as f:
        for l in f:
            if cnt > 1000:
                break
            cnt += 1
            l = l.strip('\n')
            this_dict = json.loads(l)
            main_id = this_dict['id']
            main_title = nat_get_title(mtype + '_' + str(main_id))
            if not main_title:
                continue
            top = this_dict['top'][:10]
            for subordinate_id, score in top:
                subordinate_title = nat_get_title(mtype + '_' + str(subordinate_id))
                row = [str(main_id), main_title,
                       str(subordinate_id), subordinate_title, str(score)]
                row = convert2gbk(row)
                csvwriter.writerow(row)
    fo.close()


def test18():
    # 看没有图的科普文章占比
    from general_utils.db_utils import get_newsdb_handler
    from general_utils.text_utils import filterHTML

    sql = 'select id,title,content,created_time,mini_img from news_healthnews where is_online=1;'

    o = get_newsdb_handler().do_one(sql)

    has_image_cnt = 0
    has_no_image_cnt = 0

    fo = open('news_with_no_image_info.csv', 'w')
    csvwriter = csv.writer(fo)
    first_line = ['id', 'title', 'content_len', 'created_time']
    csvwriter.writerow(first_line)

    for item in o:
        id = item[0]
        title = item[1]
        if not title:
            continue

        content = item[2]
        content = filterHTML(content)

        content_len = len(content)
        created_time = item[3]

        mini_img = item[4]

        if mini_img and len(mini_img) > 5:
            has_image_cnt += 1
            continue
        has_no_image_cnt += 1

        line = [str(id), title, str(content_len), str(created_time)]
        line = convert2gbk(line)
        csvwriter.writerow(line)

    line = ['no_image_cnt', 'has_image_cnt', 'all']
    csvwriter.writerow(line)
    line = [str(has_no_image_cnt), str(has_image_cnt), str(has_no_image_cnt + has_image_cnt)]
    csvwriter.writerow(line)
    fo.close()


def test19():
    import time
    from general_utils.db_utils import get_medicaldb_handler
    from general_utils.time_utils import timestamp2datetime
    uid = sys.argv[2]
    print 'uid', uid
    t1 = time.time()
    sql = 'select id from ask_problem where user_id=%s and created_time>"%s";' % (
        uid, timestamp2datetime(time.time() - 180 * 86400))

    o = get_medicaldb_handler().do_one(sql)
    if o is None or len(o) == 0:
        print 'nothing'
        return
    all_content = []
    for item in o:
        id = item[0]
        print id
        sql1 = 'select content from ask_problemcontent where problem_id=%s;' % id
        o1 = get_medicaldb_handler().do_one(sql1)
        all_content.append(o1)

    t2 = time.time()
    print 'time', t2 - t1


def test20():
    # 测试outlier_rejection
    import time
    from general_utils.word2vec_utils import few_points_clustering2
    input_arg = sys.argv[2]
    if '~' in input_arg:
        word_list = input_arg.split('~')
    if '-' in input_arg:
        word_list = input_arg.split('-')
    word_list = [x.decode('utf8') for x in word_list]
    vec_dict = get_vec_dict_norm_ndarray_redis(word_list)
    keep_word = outlier_rejection(vec_dict)
    print '原来的词', '-'.join(word_list)
    print '删掉的词', '-'.join(set(word_list) - set(keep_word))

    print 'test few_points_clustering'
    t1 = time.time()
    clusters = few_points_clustering2(vec_dict)
    t2 = time.time()
    print 'time', t2 - t1


def test21():
    '''
    尝试连接各种测试solr
    :return:
    '''
    from add_data_to_solr.cy_solr_local.solr_base import SolrHelper, SolrCloud, ZooKeeper
    from general_utils.solr_utils import SolrQuery
    for table in ["biztest_hospital_search",
                  "biztest_main_doctors",
                  "biztest_personal_doctors",
                  "biztest_robot_news",
                  "biztest_problem",
                  "biztest_dialog",
                  "biztest_full_problem",
                  "biztest_drug",
                  "biztest_topics",
                  "biztest_pedia"]:

        print 'tablename', table, '=' * 30
        try:
            solr = SolrCloud(ZooKeeper("rd1:2181,rd2:2181"), table)
            solr_query = SolrQuery()
            solr_query.set('q', '*:*')
            solr_query.set('fl', ['*', 'score'])
            solr_query.set('rows', 10)
            for item in solr.search(**solr_query.get_query_dict()):
                print item.get('id')
        except Exception, e:
            print e


def test22():
    from recommend.manager.feed_data_helper import select_newsid
    from random import choice
    ids_list = [[1111111, 2222222, 3333333, 4444444], [], [55, 666666], [77, 888888, 9], [1000000]]
    mode = sys.argv[2]
    num = int(sys.argv[3])
    if mode == '0':
        ids_list = choice(ids_list)

    keep_ids = select_newsid(ids_list, num)
    print 'ids_list', ids_list
    print 'selected', keep_ids


def test23():
    '''
    向solr testbiz中放"辅舒良"相关数据
    :return:
    '''

    from add_data_to_solr.cy_solr_local.solr_base import SolrHelper, SolrCloud, ZooKeeper
    from general_utils.solr_utils import SolrQuery
    alltables = ["biztest_hospital_search",
                 "biztest_main_doctors",
                 "biztest_personal_doctors",
                 "biztest_robot_news",
                 "biztest_problem",
                 "biztest_dialog",
                 "biztest_full_problem",
                 "biztest_drug",
                 "biztest_topics",
                 "biztest_pedia"]

    # robot news
    docs = [
        {'id': '9000', 'title': '辅舒良_test_news1','title_tag':['辅舒良']},
        {'id': '9001', 'title': '辅舒良_test_news2','title_tag':['辅舒良']},
    ]
    solr_news = SolrCloud(ZooKeeper("rd1:2181,rd2:2181"), "biztest_robot_news")
    solr_news.add(docs)

    # topics
    docs = [
        {'id': '510', 'title': '辅舒良_test_news1', 'content': '辅舒良'},
        {'id': '511', 'title': '辅舒良_test_news2', 'content': '辅舒良'},
    ]

    solr_topics = SolrCloud(ZooKeeper("rd1:2181,rd2:2181"), "biztest_topics")
    solr_topics.add(docs,fieldUpdates={"title":"set","content":"set"})




def test24():
    from recommend.consts import ALL_CLINIC_CHOICES
    from rpc_services.search_api import get_doctor_topN_diseases,search_doctors
    clinic_hot_diseases = defaultdict(list)
    for clinic_no,_,text in ALL_CLINIC_CHOICES:
        try:
            clinic_no = int(clinic_no)
            text = unicode(text)
            top50_did = search_doctors(text,100)
            for did in top50_did:
                did_hot_diseases_info_list = get_doctor_topN_diseases(did)
                clinic_hot_diseases[text].extend([item[0] for item in did_hot_diseases_info_list])
        except:
            continue


    for text in clinic_hot_diseases:

        this_tags = set(clinic_hot_diseases[text])
        with open('pmdata/' + text + '.txt','w') as f:
            for clinic_text in clinic_hot_diseases:
                if clinic_text == text:
                    continue
                tags = set(clinic_hot_diseases[clinic_text])
                cross_tags = tags & this_tags
                if cross_tags:
                    f.write(clinic_text + '\t' + '|'.join(cross_tags) + '\n')

def test25():
    '''
    往solr biz 里放一些数据
    :return:
    '''
    from add_data_to_solr.cy_solr_local.solr_base import SolrHelper, SolrCloud, ZooKeeper
    from general_utils.solr_utils import SolrQuery
    table = "biztest_full_problem"
    solr = SolrCloud(ZooKeeper("rd1:2181,rd2:2181"), table)

    def get_ids():
        solrQuery = SolrQuery()
        solrQuery.set("q","*:*")
        solrQuery.add("fq","doctor_id:clinic_web_c383b3a7e6db1f1d")
        solrQuery.set("fl",["id"])
        solrQuery.set("rows",200)
        res = [item["id"] for item in solr.search(**solrQuery.get_query_dict())]
        return res

    ids = get_ids()
    print len(ids)
    docs = []
    for id in ids:
        docs.append({"id":id,"diseases":["痔疮","皮炎","高血压","吃的太多"]})





    solr.add(docs, fieldUpdates={"diseases": "set"})


def test26():
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=30000)
    table = connection.table("problem")
    start = '2018-03-26'
    end = '2018-03-29'
    d = defaultdict(set)
    for key,value in table.scan(row_start=start,row_stop=end):
        date,uid,pid = key.split('_')
        close_dt = value["detail:close_time"]
        close_date = close_dt.split()[0]
        d[date].add(close_date)

    for date in d:
        print date,d[date]


def test27():
    from add_data_to_solr.cy_solr_local.solr_base import SolrHelper, SolrCloud, ZooKeeper
    from general_utils.solr_utils import SolrQuery
    solr = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "main_doctors")
    for id in ("clinic_web_98f32ad0d4461af8",
               "clinic_web_63da2e8135fabfb1",
               "clinic_zhongyike_zhouyuchun",
               "clinic_web_7ff76f1118d806e6",
               "4647c810af1ee0850bf2"):
        sq = SolrQuery()
        sq.set('q','*:*')
        sq.add('fq','id:%s'%id)
        sq.set('rows',1)
        sq.set('fl',['*'])
        res = [item for item in solr.search(**sq.get_query_dict())][0]
        if 'name' not in res:
            continue
        print '='*30
        print id,res.get('name2','')
        for key in res:
            if 'score' in key or 'rate' in key or 'star' in key:
                print key,res[key]


def test28():
    pass

if __name__ == '__main__':
    index = sys.argv[1]
    eval("test%s()" % index)
