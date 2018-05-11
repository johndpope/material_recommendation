# encoding=utf8


import os
import sys
import json
import time
import math
from random import shuffle
import happybase
from global_config import get_root_path

from recommend.manager.data_helper import get_sex, population_cons2, population_limits, \
    find_bdpart, rank_news, weight_dict_normalization
from recommend.manager.redis_utils import Word2VecCache

from general_utils.db_utils import get_medicaldb_handler
from general_utils.hbase_utils import get_user_query3, get_news_id_from_cy_event2, user_half_year_newsids
from general_utils.solr_utils import get_cy_event_row_key_news, get_cy_event_row_key_topic, \
    more_news_from_solr_nat, get_caled_user_topn_news
from general_utils.time_utils import timestamp2datetime, ensure_second_timestamp, datetime_str2timestamp
from general_utils.word2vec_utils import few_points_clustering2

from rpc_services.medical_service_utils import get_weighted_word_list
from rpc_services.word2vec_api import get_vec_dict_norm_ndarray_redis
from rpc_services.qa_promotion_api import get_special_population

half_month_active_uids_file = os.path.join(get_root_path(), 'recommend/data_dir', 'half_month_uids')


def first_online_tasks():
    '''
    1 找出半个月内有活动的用户，跑数据，存起来（solr),约50w
    :return:
    '''
    # step1 获取半个月有活动的用户uid，"活动"指的是qa,bs(包括找医生),阅读news,阅读topic
    # qa -> mysql; bs,view_news,view_topic -> solr + hbase
    end_ts = time.time()
    begin_ts = end_ts - 86400.0 * 30  # 半个月
    # 1.1 qa
    print 'getting qa uids....'
    qa_uids_set = get_qa_uids(begin_ts, end_ts)
    # 1.2 query
    print 'getting query uids....'
    query_uids_set = get_query_uids(begin_ts, end_ts, mode='search')
    # 1.3 view
    print 'gettting news uids....'
    news_uids_set = get_query_uids(begin_ts, end_ts, mode='news')
    print 'getting topic uids....'
    topic_uids_set = get_query_uids(begin_ts, end_ts, mode='topic')

    all_uids = qa_uids_set
    all_uids.update(query_uids_set)
    all_uids.update(news_uids_set)
    all_uids.update(topic_uids_set)

    print 'all_uids num', len(all_uids)
    all_uids = [str(x) for x in all_uids]
    with open(half_month_active_uids_file, 'w') as f:
        f.write('\n'.join(all_uids))


def first_online_tasks2():
    '''
    cal uid recommend news
    :return:
    '''
    file_name = sys.argv[1]
    all_uids = []
    with open(file_name, 'r') as f:
        for l in f:
            uid = int(l.strip('\n'))
            all_uids.append(uid)
    print '%s uid num=%s' % (file_name, len(all_uids))
    fo = open(file_name + '.ids', 'w')
    check_batch = 10000
    cnt = 0
    for uid in all_uids:
        res = recommend_news_kernel(uid)
        ids = res['ids']
        info_dict = {'uid': uid, 'ids': ids}
        info_json = json.dumps(info_dict)
        fo.write(info_json + '\n')
        cnt += 1
        if cnt % check_batch == 0:
            print cnt

    fo.close()


def get_qa_uids(begin, end):
    # 获取begin-end之间所有qa对应的user_id
    begin_dt = timestamp2datetime(ensure_second_timestamp(begin))
    end_dt = timestamp2datetime(ensure_second_timestamp(end))
    sql = 'select distinct user_id from ask_problem where created_time>"%s" and created_time<"%s";' % (begin_dt, end_dt)
    o = get_medicaldb_handler().dbhandler.do_one(sql)
    uids = set()
    for item in o:
        uid = item[0]
        uids.add(int(uid))
    return uids


def get_query_uids(begin, end, mode='search'):
    from general_utils.solr_utils import zk_md4, SolrQuery, pysolr
    from general_utils.time_utils import ensure_m_timestamp
    # 更改时间戳格式
    begin = ensure_m_timestamp(begin)
    end = ensure_m_timestamp(end)
    # 构建索引
    q = '*:*'
    solr_query = SolrQuery()
    solr_query.set('q', q)
    solr_query.add('fq', 'event_time:[%s TO %s]' % (begin, end))
    solr_query.set('fl', ['uid'])
    solr_query.set('rows', 1000000)

    if mode == 'search':
        solr = pysolr.SolrCloud(zk_md4, 'search_event', timeout=25)
    elif mode == 'news':
        solr = pysolr.SolrCloud(zk_md4, 'news_profile', timeout=25)
    elif mode == 'topic':
        solr = pysolr.SolrCloud(zk_md4, 'topic_profile', timeout=25)
    else:
        return set()

    uid_list = [int(item['uid']) for item in solr.search(**solr_query.get_query_dict())]
    uid_set = set(uid_list)
    print 'len uids', len(uid_set)
    return uid_set


###########################################
# 将half_month_active_uids_file里边的uid的数据获取了，存起来(啊啊啊啊啊啊好慢，直接从cy_real_time_event里取了）
# 另一方面把这些数据里的文本分词去停止词后存起来（跟uid无关），先看看有多少词（10w左右应该阔以吧。。。）

half_month_active_userinfo_file = os.path.join(get_root_path(), 'recommend/data_dir', 'half_month_userinfo.json')


def save_user_info():
    # first online 时候用
    with open(half_month_active_uids_file, 'r') as f:
        uids = f.readlines()
    uids = [x.strip('\n') for x in uids]

    print 'uids num', len(uids)
    cnt = 0
    t0 = time.time()
    with open(half_month_active_userinfo_file, 'w') as f:
        for uid in uids:
            if cnt % 10000 == 0:
                t = time.time()
                print 'cnt', cnt, t - t0
            user_info_list = get_user_info(uid)
            json_line = json.dumps({'uid': uid, 'info': user_info_list})
            f.write(json_line + '\n')
            cnt += 1


def save_user_info2():
    '''
    词聚类用
    从habse cy_real_time_event里搞一些query和qa_ask作为语料，分词，转换成词向量（晚上跑）
    :return:
    '''
    from general_utils.hbase_utils import get_all_realtimeevent_text
    texts = get_all_realtimeevent_text()
    text_tf_list = [[x, 1.0] for x in texts]
    weight_dict, cate_dict = get_weighted_word_list(text_tf_list)
    print 'words num', len(weight_dict.keys())


###########################################



def get_qa_text(uid, begin, end, num):
    # 需要快，同时保留事件的时间
    bad_return = [], []
    begin = ensure_second_timestamp(begin)
    end = ensure_second_timestamp(end)
    sql = 'select id,created_time,ask from ask_problem  where user_id=%s order by id desc limit %s;' % (uid, num)
    # print 'sql', sql
    o = get_medicaldb_handler().do_one(sql)
    if o is None or len(o) == 0:
        return bad_return

    text_list = []
    ts_list = []
    for item in o:
        dt = str(item[1])
        ts = datetime_str2timestamp(dt)
        if ts < begin or ts > end:
            continue
        first_ask = unicode(item[2])
        text_list.append(first_ask)
        ts_list.append(ts)
    return text_list, ts_list


def get_query_text(uid, begin, end, num):
    # 需要快，先从cy_real_time_event取，没有再从solr取key
    # 找医生的info:query可能是科室名
    pass


def get_user_all_info_realtimeevent(uid, qa_num=3, bs_num=5, sd_num=5, news_num=5, topic_num=5):
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=30000)
    table = connection.table("cy_real_time_event")
    qa_cnt = 0
    bs_cnt = 0
    sd_cnt = 0
    news_cnt = 0
    topic_cnt = 0

    # 每类事件最早时间戳ms
    qa_earliest_ts = int(time.time() * 1000)
    bs_earliest_ts = qa_earliest_ts
    sd_earliest_ts = qa_earliest_ts
    news_earliest_ts = qa_earliest_ts
    topic_earliest_ts = qa_earliest_ts

    res = []  # [ts,obj,action_type]

    o = [item for item in table.scan(row_prefix=str(uid) + '|')][::-1]
    connection.close()

    for key, value in o:
        _, ts, action_type = key.split('|')
        ts = int(ts)
        if 'free_problem' in action_type:
            # qa
            if qa_cnt >= qa_num:
                continue
            obj = unicode(value['info:ask'])
            action_type1 = 'qa'
            qa_cnt += 1
            if ts < qa_earliest_ts:
                qa_earliest_ts = ts
        elif 'big_search' in action_type:
            if bs_cnt >= bs_num:
                continue
            obj = unicode(value['info:query'])
            action_type1 = 'bs'
            bs_cnt += 1
            if ts < bs_earliest_ts:
                bs_earliest_ts = ts
        elif 'search_doctor' in action_type:
            if sd_cnt >= sd_num:
                continue
            obj = unicode(value['info:query'])
            action_type1 = 'sd'
            sd_cnt += 1
            if ts < sd_earliest_ts:
                sd_earliest_ts = ts
        elif 'view_news' in action_type:
            if news_cnt >= news_num:
                continue
            obj = int(value['info:news_id'])
            action_type1 = 'vn'
            news_cnt += 1
            if ts < news_earliest_ts:
                news_earliest_ts = ts
        elif 'view_topic' in action_type:
            if topic_cnt >= topic_num:
                continue
            obj = int(value['info:topic_id'])
            action_type1 = 'vt'
            topic_cnt += 1
            if ts < topic_earliest_ts:
                topic_earliest_ts = ts
        else:
            continue
        res.append([ts, obj, action_type1])

    earliest_ts_dict = {
        'qa': qa_earliest_ts,
        'bs': bs_earliest_ts,
        'sd': sd_earliest_ts,
        'vn': news_earliest_ts,
        'vt': topic_earliest_ts

    }

    cnt_dict = {
        'qa': qa_cnt,
        'bs': bs_cnt,
        'sd': sd_cnt,
        'vn': news_cnt,
        'vt': topic_cnt
    }

    return res, earliest_ts_dict, cnt_dict


def get_additional_qa_info(uid, qa_num, earliest_ts):
    # from mysql
    earliest_ts += -1000  # 1s内视为同一个动作
    qa_text_list, qa_ts_list = get_qa_text(uid, 0, time.time(), qa_num)
    qa_keep_index = [i for i in range(len(qa_ts_list)) if int(qa_ts_list[i] * 1000) < earliest_ts]
    qa_res = [[int(1000 * qa_ts_list[i]), qa_text_list[i], 'qa'] for i in qa_keep_index]
    return qa_res


def get_additional_query_info(uid, bs_num, earliest_ts):
    # bs and sd from solr and hbase
    earliest_ts += -1000  # 1s内视为同一个动作
    text_dict, ts_dict, _ = get_user_query3(uid, num=20)
    # for x in text_dict:
    #     print 'text_dict', x, text_dict[x]
    # for x in ts_dict:
    #     print 'ts_dict', x, ts_dict[x]

    bs_keep_key = [key for key in ts_dict if ts_dict[key] < earliest_ts]
    bs_res = [[ts_dict[key], text_dict[key], 'bs'] for key in bs_keep_key]
    selected_bs_res = sorted(bs_res, key=lambda x: x[0], reverse=True)[:bs_num]
    return selected_bs_res


def get_additional_viewnews_info(uid, news_num, earliest_ts):
    # view news from solr and hbase
    earliest_ts += -1000  # 1s内视为同一个动作
    news_row_key_list = [item['id'] for item in get_cy_event_row_key_news(uid, news_num)]
    news_ts_dict = get_news_id_from_cy_event2(news_row_key_list, col='info:news_id')
    news_keep_key = [key for key in news_ts_dict if news_ts_dict[key] < earliest_ts]
    news_res = [[news_ts_dict[key], int(key.split('_')[1]), 'vn'] for key in news_keep_key]
    return news_res


def get_additional_viewtopic_info(uid, topic_num, earliest_ts):
    # view topic from solr and hbase
    earliest_ts += -1000  # 1s内视为同一个动作
    topic_row_key_list = [item['id'] for item in get_cy_event_row_key_topic(uid, topic_num)]
    topic_ts_dict = get_news_id_from_cy_event2(topic_row_key_list, col='info:topic_id')
    topic_keep_key = [key for key in topic_ts_dict if topic_ts_dict[key] < earliest_ts]
    topic_res = [[topic_ts_dict[key], int(key.split('_')[1]), 'vt'] for key in topic_keep_key]
    return topic_res


def get_user_info(uid, qa_num=3, bs_num=5, sd_num=5, news_num=5, topic_num=5):
    # 需要快，获取用户最后若干个信息

    realtime_user_info_list, earliest_ts_dict, cnt_dict = \
        get_user_all_info_realtimeevent(uid, qa_num=qa_num,
                                        bs_num=bs_num,
                                        sd_num=sd_num,
                                        news_num=news_num,
                                        topic_num=topic_num)

    if cnt_dict['qa'] < qa_num:
        qa_add = get_additional_qa_info(uid, qa_num, earliest_ts_dict['qa'])
        realtime_user_info_list.extend(qa_add)

    if cnt_dict['bs'] < bs_num:
        bs_add = get_additional_query_info(uid, bs_num - cnt_dict['bs'], earliest_ts_dict['bs'])
        realtime_user_info_list.extend(bs_add)

    if cnt_dict['vn'] < news_num:
        news_add = get_additional_viewnews_info(uid, news_num, earliest_ts_dict['vn'])
        realtime_user_info_list.extend(news_add)

    if cnt_dict['vt'] < topic_num:
        topic_add = get_additional_viewtopic_info(uid, topic_num, earliest_ts_dict['vt'])
        realtime_user_info_list.extend(topic_add)

    return realtime_user_info_list


k = math.log(10) / 30.0 / 86400.0  # 30天衰减至原来的十分之一


def time_factor(ts, t0):
    # 牛顿冷却定律的时间衰减因子
    return math.exp(- k * abs(ensure_second_timestamp(t0) - ensure_second_timestamp(ts)))


def get_user_special_population(user_info_list):
    '''
    优先从qa信息中取，否则从query里取

    :param user_info_list:
    :return:
    '''
    qa_text = [item[1] for item in user_info_list if item[2] == 'qa']
    if qa_text:
        text = ''.join(qa_text)
        sp = get_special_population(''.join(text))
        sex = get_sex(text)
        return sp, sex

    query_text = [item[1] for item in user_info_list if item[2] in ('bs', 'sd')]
    if query_text:
        text = ''.join(query_text)
        sp = get_special_population(text)
        sex = get_sex(text)
        return sp, sex

    return 'common_population', 0


def parse_user_info_feed(user_info_list):
    '''
    将文本分词，识别为医学实体，变成向量，给出权重
    :param user_info_list:
    :return:
    '''

    # 将user_info 分成 view_actions 和 带文字的 text_actions
    view_actions = [item for item in user_info_list if item[2] in ('vn', 'vt')]
    text_actions = [item for item in user_info_list if item[2] not in ('vn', 'vt')]

    # 将时间戳按照time_factor的方法转化成权重因子.
    T0 = time.time()
    text_tf_list = [[item[1], time_factor(item[0], T0)] for item in text_actions if item[1]]
    # 调整time factor使最大值为1.0
    max_tf = max([item[1] for item in text_tf_list]) if text_tf_list else 1.0
    text_tf_list = [[item[0], item[1] / max_tf] for item in text_tf_list]
    # 分词
    word_weight_dict, cate_dict = get_weighted_word_list(text_tf_list)
    # 转换成词向量
    word_vec_dict = get_vec_dict_norm_ndarray_redis(word_weight_dict.keys())

    # 获取特殊人群和性别
    sp, sex = get_user_special_population(user_info_list)
    # 返回结果
    parsed_user_info = {
        'weight_dict': word_weight_dict,
        'cate_dict': cate_dict,
        'vec_dict': word_vec_dict,
        'view_actions': view_actions,
        'text_actions': text_actions,
        'text_tf_list': text_tf_list,
        'sp': sp,
        'sex': sex,
    }
    return parsed_user_info


def clean_user_word(user_vec_dict, weight_dict):
    '''
    1 获得各个cluster
    2 按照每个cluster的总权重排序，取最大的
    :param user_vec_dict:
    :return:
    '''
    clusters = few_points_clustering2(user_vec_dict)
    cluster_score = [[0.0, i] for i in range(len(clusters))]
    for i, cluster in enumerate(clusters):
        for word in cluster:
            cluster_score[i][0] += weight_dict[word]
    sorted_cluster_score = sorted(cluster_score, key=lambda x: x[0], reverse=True)

    # print res
    for score, i in sorted_cluster_score:
        print 'sorted_cluster_score', '-'.join(clusters[i]), score

    return clusters[sorted_cluster_score[0][1]] if sorted_cluster_score else set()


def split_user_word(user_vec_dict, weight_dict, num=2):
    clusters = few_points_clustering2(user_vec_dict)
    cluster_score = [[0.0, i] for i in range(len(clusters))]
    for i, cluster in enumerate(clusters):
        for word in cluster:
            cluster_score[i][0] += weight_dict[word]
    sorted_cluster_score = sorted(cluster_score, key=lambda x: x[0], reverse=True)

    # print res
    for score, i in sorted_cluster_score:
        print 'sorted_cluster_score', '-'.join(clusters[i]), score
    if not sorted_cluster_score:
        return []
    return [clusters[item[1]] for item in sorted_cluster_score[:num]]


def recall_and_rank(uid, parsed_user_info, o_vec_dict, o_weight_dict, word_cluster, num):
    new_vec_dict = {}
    new_weight_dict = {}
    for tag in word_cluster:
        new_vec_dict[tag] = o_vec_dict[tag]
        new_weight_dict[tag] = o_weight_dict[tag]
    parsed_user_info['vec_dict'] = new_vec_dict
    parsed_user_info['weight_dict'] = new_weight_dict
    # recall news
    recall_ids, title_dict, score_dict = recall_news_feed(uid, parsed_user_info)
    # rank news
    id_score_list, v_score_dict = rank_news_feed(uid=uid,
                                                 parsed_user_info=parsed_user_info,
                                                 recall_ids=recall_ids,
                                                 solr_score_dict=score_dict,
                                                 num=num)
    return recall_ids, title_dict, id_score_list, v_score_dict


def recommend_news_kernel(uid, test=False, num=10):
    '''
    取uid 最近的几条数据 作为用户数据，根据套路给出推荐的topn文章（暂时topn=10吧）
    :param uid:
    :param begin:
    :param end:
    :return:
    '''

    # raw数据
    user_info_list = get_user_info(uid)
    # parse
    parsed_user_info = parse_user_info_feed(user_info_list)
    # clean user tags
    vec_dict = parsed_user_info['vec_dict']
    weight_dict = parsed_user_info['weight_dict']
    # clean_tags_set = clean_user_word(vec_dict, weight_dict)

    word_clusters = split_user_word(vec_dict, weight_dict, 4)

    # new_vec_dict = {}
    # new_weight_dict = {}
    # for tag in clean_tags_set:
    #     new_vec_dict[tag] = vec_dict[tag]
    #     new_weight_dict[tag] = weight_dict[tag]
    # parsed_user_info['vec_dict'] = new_vec_dict
    # parsed_user_info['weight_dict'] = new_weight_dict
    # # recall news
    # recall_ids, title_dict, score_dict = recall_news_feed(uid, parsed_user_info)
    # # rank news
    # id_score_list, v_score_dict = rank_news_feed(uid=uid,
    #                                              parsed_user_info=parsed_user_info,
    #                                              recall_ids=recall_ids,
    #                                              solr_score_dict=score_dict,
    #                                              num=num)
    all_ids = []
    title_dict = {}
    recall_ids = []
    v_score_dict = {}
    for word_cluster in word_clusters:
        t_recall_ids, t_title_dict, id_score_list, t_v_score_dict = recall_and_rank(
            uid=uid,
            parsed_user_info=parsed_user_info,
            o_vec_dict=vec_dict,
            o_weight_dict=weight_dict,
            word_cluster=word_cluster,
            num=num
        )

        ids = [int(item[0].split('_')[1]) for item in id_score_list if t_v_score_dict[item[0]] >= 0.1][:num]
        if ids:
            all_ids.append(ids)
        if test:
            title_dict.update(t_title_dict)
            recall_ids.append(t_recall_ids)
            v_score_dict.update(t_v_score_dict)

    res = {
        'ids': all_ids
    }

    if test:
        res['parsed_user_info'] = parsed_user_info
        res['recall_ids'] = recall_ids
        res['title_dict'] = title_dict
        res['v_score_dict'] = v_score_dict
        res['user_info_list'] = user_info_list

    return res


def rank_news_feed(uid, parsed_user_info, recall_ids, solr_score_dict, num=10):
    uid = int(uid)
    score_dict = {}
    v_score_dict = {}
    # title_dict = {}
    weight_dict = parsed_user_info['weight_dict']
    u_tags = weight_dict.keys()
    u_bp_words = find_bdpart(u_tags)
    vec_dict = parsed_user_info['vec_dict']
    u_vecs_keys = vec_dict.keys()
    u_vecs = [vec_dict[word] for word in u_vecs_keys]
    u_weights = [weight_dict[word] for word in u_vecs_keys]

    print "u_bp_words", '+'.join(u_bp_words)

    # calculate news similarities

    rank_news(news_ids=recall_ids,
              solr_score_dict=solr_score_dict,
              score_dict=score_dict,
              v_score_dict=v_score_dict,
              uid=uid,
              u_vecs=u_vecs,
              u_weights=u_weights,
              u_bp_words=u_bp_words)
    sorted_id_score = sorted(score_dict.iteritems(), key=lambda x: x[1], reverse=True)

    return sorted_id_score[:num], v_score_dict


def recall_news_feed(uid, parsed_user_info):
    # texts = [item[1] for item in user_info_list if item[2] in ('qa', 'bs', 'sd')]
    text_tf_list = parsed_user_info['text_tf_list']
    for x, y in text_tf_list:
        print x, y
    weight_dict = parsed_user_info['weight_dict']
    tags = weight_dict.keys()
    weights = [weight_dict[word] for word in tags]
    # cate_dict = parsed_user_info['cate_dict']
    sp = parsed_user_info['sp']
    news_cons = population_cons2(sp)  # 不能在这些分类
    news_limits = population_limits(sp)  # 必须在这些分类
    # drug_words = [x for x in tags if (x in cate_dict and cate_dict[x] == 'DRUG_DESC')]

    recall_ids, title_dict, score_dict = more_news_from_solr_nat(text_tf_list=text_tf_list,
                                                                 tags=tags,
                                                                 weights=weights,
                                                                 rows=25,
                                                                 news_cons=news_cons,
                                                                 news_limits=news_limits,
                                                                 )

    # 去掉半年浏览过的id
    half_year_news_ids = user_half_year_newsids(uid)
    filtered_recall_ids = [item for item in recall_ids
                           if int(item.split('_')[1]) not in half_year_news_ids]

    for x in filtered_recall_ids:
        print 'recall', x, title_dict[x], score_dict[x]
    return filtered_recall_ids, title_dict, score_dict


def get_all_yesterday_user_id(now=None, test=False):
    from general_utils.hbase_utils import get_sp_duration_active_userid
    from general_utils.time_utils import get_yesterday_timestamp
    if not now:
        now = time.time()
    else:
        now = ensure_second_timestamp(now)

    begin, end = get_yesterday_timestamp(now)
    if test:
        end = begin + 30 * 60  # 测试模式只取三十分钟数据

    all_valid_uids = get_sp_duration_active_userid(begin, end)
    return all_valid_uids


MIN_NEWS_ID = 60551


def new_newsids_check(ids, num):
    '''
    去掉2016年之前的文章
    :param ids:
    :param num: 保留数目
    :return:
    '''
    keep_ids = []
    for id in ids:
        if len(keep_ids) >= num:
            return keep_ids
        if id >= MIN_NEWS_ID:
            keep_ids.append(id)
    return keep_ids


def new_newsids_check_random(ids, num):
    keep_ids0 = [id for id in ids if id >= MIN_NEWS_ID]
    shuffle(keep_ids0)
    return keep_ids0[:num]


def select_newsid(ids_list, num, bad_ids=None):
    '''
    1 从前两个主题随机平均选购num个
    '''

    #
    if not ids_list:
        return []

    if bad_ids is None:
        bad_ids = []

    if isinstance(ids_list[0], int):
        # 旧版的ids_list只有一层 [id1,id2,...]
        # 去掉推过的
        ids_list = [x for x in ids_list if x not in bad_ids]
        return new_newsids_check_random(ids_list, num)

    # 新版的ids_list有两层 [[id1,id2],[id3,id4,id5],...]
    # outer shuffle
    shuffle(ids_list)

    max_len = max([len(ids) for ids in ids_list])
    # padding and inner shuffle
    for ids in ids_list:
        shuffle(ids)
        ids += [0] * (max_len - len(ids))
        # select
    keep_ids = []
    for i in range(max_len):
        for j in range(len(ids_list)):
            pick_id = ids_list[j][i]
            if pick_id and pick_id not in keep_ids and pick_id not in bad_ids:
                keep_ids.append(pick_id)
            if len(keep_ids) >= num:
                return keep_ids
    return keep_ids


def get_caled_user_topn_news_from_solr(uid):
    return get_caled_user_topn_news(uid)


def clean_caled_ids(caled_ids):
    if not caled_ids:
        return caled_ids, False
    if isinstance(caled_ids[0], int):
        return [x for x in caled_ids if x >= MIN_NEWS_ID], False
    return caled_ids, True


def get_ramdom_topn_news_from_solr(uid, num):
    # solr 存的推荐id
    solr_caled_ids = get_caled_user_topn_news_from_solr(uid)
    # redis 存的已推荐id
    redis_ids = Word2VecCache.get_user_showed_news(uid)
    # 若solr_caled_ids 都在 redis_ids中，则清空redis中的id，重新开始

    cleaned_caled_ids, is_new_format = clean_caled_ids(solr_caled_ids)
    if is_new_format is False:
        clean_id_set = set(cleaned_caled_ids)
    else:
        clean_id_set = set()
        for id_list in cleaned_caled_ids:
            clean_id_set.update(set(id_list))
    if len(clean_id_set - set(redis_ids)) == 0:
        push_ids = select_newsid(cleaned_caled_ids, num)
        write_user_showed_news_to_redis(uid, push_ids)
        return push_ids
    push_ids = select_newsid(cleaned_caled_ids, num, redis_ids)
    redis_ids.extend(push_ids)
    write_user_showed_news_to_redis(uid, redis_ids)
    return push_ids


def get_random_topn_news_from_redis(uid, num):
    # 给那些没有被离线计算过的用户提供短期的存储
    redis_ids_all = Word2VecCache.get_user_all_news(uid)
    redis_ids_showed = Word2VecCache.get_user_showed_news(uid)
    if not redis_ids_all:
        return []
    all_id_set = set()
    for id_list in redis_ids_all:
        all_id_set.update(set(id_list))
    if len(all_id_set - set(redis_ids_showed)) == 0:
        push_ids = select_newsid(redis_ids_all, num)
        write_user_showed_news_to_redis(uid, push_ids)
        return push_ids
    push_ids = select_newsid(redis_ids_all, num, redis_ids_showed)
    redis_ids_showed.extend(push_ids)
    write_user_showed_news_to_redis(uid, redis_ids_showed)
    return push_ids


def write_user_all_news_to_redis(uid, id_list):
    return Word2VecCache.set_user_all_news(uid, id_list)


def write_user_showed_news_to_redis(uid,ids):
    return Word2VecCache.set_user_showed_news(uid,ids)

def test():
    uid = sys.argv[1]
    t1 = time.time()
    solr_ids = Word2VecCache.get_user_all_news(uid)
    before_redis_ids = Word2VecCache.get_user_showed_news(uid)
    push_ids = get_random_topn_news_from_redis(uid, 2)
    after_redis_ids = Word2VecCache.get_user_showed_news(uid)
    print 'push_ids', push_ids
    print 'solr_ids', solr_ids
    print 'before_redis_ids', before_redis_ids
    print 'after_redis_ids', after_redis_ids

    t2 = time.time()
    print 't', t2 - t1


if __name__ == '__main__':
    test()
