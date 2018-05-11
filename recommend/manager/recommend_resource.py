# encoding=utf8
# 输入uid，输出推荐的文章id和title
# never put test case in this script

import os
import time
import json
from random import shuffle

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
from settings import IS_ONLINE_WEB_SERVER
from random import choice
from general_utils.hbase_utils import cy_time_event_one_user_kernel
from recommend.manager.data_helper import parse_user_info, filter_user_info, recall, rank, aftertreatment, \
    trigger_threshhold, one_user_last_qa_info, is_valid_user_info
from recommend.manager.recommend_tags_data_helper import get_similar_entities, \
    get_relation_plan, get_user_last_query, get_relation_plan3, systagid_2_planid
from recommend.manager.recommend_topic_data_helper import parse_user_info as parse_user_info2
from recommend.manager.recommend_topic_data_helper import get_yesterday_user_info, get_caled_user_topn_topics_yesterday0
from recommend.manager.feed_data_helper import recommend_news_kernel, get_ramdom_topn_news_from_solr, select_newsid, \
    get_random_topn_news_from_redis, write_user_all_news_to_redis, write_user_showed_news_to_redis

from general_utils.time_utils import ensure_second_timestamp
from medweb_utils.utilities.log_utils import info_logger

TEST_RETURN_Recommend = [[1444, u"为啥我是个话题的标题？", "topic"], [8698, u"为啥我竟然是个科普文章的标题而不是话题的标题？", "news"]]


def Recommend(uid, lookback, end=None, pid=None):
    # if not IS_ONLINE_WEB_SERVER:
    #     return choice(TEST_RETURN)

    # recommed top 1
    bad_return = [-1, "", "nothing"]  # material_id, title, material_type
    log_mark = "recommend_one"
    info_logger.info("%s===============start=========uid=%s==============pid=%s===============", log_mark, uid,
                     str(pid))

    try:
        uid = int(uid)
    except:
        info_logger.info("%s=====failed in recommend==bad uid=%s=========", log_mark, uid)
        return bad_return

    if uid == -1:
        info_logger.info("%s=====failed in recommend==bad uid=%s=========", log_mark, uid)
        return bad_return

    if not end:
        end = time.time()
    else:
        end = ensure_second_timestamp(end)

    begin = end - lookback

    end += 5.0  # 结束点顺延5s，防止hbase表里还没有实时数据

    if pid:  # qa触发由传入的problem_id查询信息
        user_info0 = one_user_last_qa_info(pid)
    else:
        user_info0 = cy_time_event_one_user_kernel(uid, begin, end)

    res_dict = Recommend_by_user_info(user_info0, uid, log_mark=log_mark)
    res = res_dict['res']
    status = res_dict['status']
    if not res:
        info_logger.info("%s==failed in recommend==%s===uid=%s===========", log_mark, status, uid)
        return bad_return
    best_id, title, mtype = res[0]
    info_logger.info("%s==succeed in recommend===id=%s==title=%s====type=%s===uid=%s===========", log_mark, best_id,
                     title, mtype, uid)
    return [int(best_id), title, mtype]


TEST_RETURN_Recommend_list = [{'id': 1444, 'title': u"为啥我是个话题的标题？", 'type': "topic"},
                              {'id': 1460, 'title': u"i am anther topic title", 'type': "topic"},
                              {'id': 1455, 'title': u"i am anther topic title", 'type': "topic"},
                              {'id': 8698, 'title': u"为啥我竟然是个科普文章的标题而不是话题的标题？", 'type': "news"}]


def Recommend_list(uid, num, end=None, pid=None, lookback=5 * 61.0):
    # return : [{'id':111,'type':'topic','title':'xxxx'},{'id':222,'type':'news','title':'yyy'}...]
    # ******************************************
    # if not IS_ONLINE_WEB_SERVER:
    #     return TEST_RETURN_Recommend_list
    # ******************************************

    bad_return = []
    log_mark = "recommend_topn"
    info_logger.info("%s===============start=========uid=%s==============pid=%s===============", log_mark, uid,
                     str(pid))

    # assert uid
    try:
        uid = int(uid)
    except:
        info_logger.info("%s=====failed in recommend==bad uid=%s=========", log_mark, uid)
        return bad_return
    if uid == -1:
        info_logger.info("%s=====failed in recommend==bad uid=%s=========", log_mark, uid)
        return bad_return

    # time window
    if not end:
        end = time.time()
    else:
        end = ensure_second_timestamp(end)

    begin = end - lookback

    end += 5.0  # 结束点顺延5s，防止hbase表里还没有实时数据
    if pid:  # qa触发由传入的problem_id查询信息
        user_info0 = one_user_last_qa_info(pid)
    else:
        user_info0 = cy_time_event_one_user_kernel(uid, begin, end)

    res_dict = Recommend_by_user_info(user_info0, uid, log_mark=log_mark, num=num)
    res = res_dict['res']
    status = res_dict['status']
    if not res:
        info_logger.info("%s==failed in recommend==%s===uid=%s===========", log_mark, status, uid)
        return bad_return
    for item in res:
        best_id, title, mtype = item
    info_logger.info("%s==succeed in recommend===id=%s==title=%s====type=%s===uid=%s===========", log_mark, best_id,
                     title, mtype, uid)
    return [{'id': item[0], 'title': item[1], 'type': item[2]} for item in res]


def Recommend_by_user_info(user_info0, uid, log_mark, num=1, test=False):
    if test:
        bad_return = {"user_info": None, "res": None, "topn_ids_scores": None, "only_topic": None, "v_score_dict": None}
    else:
        bad_return = {"res": None}
    t1 = time.time()
    user_info = parse_user_info(user_info0)
    t2 = time.time()
    print "Recommend_by_user_info parse_user_info time", t2 - t1

    if user_info is None:
        bad_return["status"] = "user info is None"
        return bad_return
    if not is_valid_user_info(user_info):
        bad_return["status"] = "not valid user info"
        return bad_return

    only_topic = False

    if not filter_user_info(user_info):
        only_topic = True

    texts = user_info["texts"]
    tags = user_info["tags"]
    special_population = user_info["special_population"]
    center = user_info["center"]
    u_vecs = user_info["vecs"]
    u_weights = user_info["weights"]
    u_cates = user_info["cates"]
    trigger = user_info["trigger"]

    info_logger.info("%s=texts=%s======uid=%s===============", log_mark, '|||'.join(texts), uid)
    info_logger.info("%s=tags=%s======uid=%s===============", log_mark, '|||'.join(tags), uid)
    info_logger.info("%s=special_population=%s======uid=%s===============", log_mark, special_population, uid)
    info_logger.info("%s=trigger=%s======uid=%s===============", log_mark, trigger, uid)
    info_logger.info("%s=only_topic=%s======uid=%s===============", log_mark, only_topic, uid)
    ###############
    if trigger not in ("big_search", "free_problem_create"):
        bad_return["status"] = "trigger_type not bs or qa"
        return bad_return
    ################



    # 召回
    t3 = time.time()
    recall_ids, title_dict, score_dict = recall(text=' '.join(texts), tags=tags,
                                                weights=u_weights,
                                                cates=u_cates,
                                                special_population=special_population,
                                                center=center, trigger_type=trigger,
                                                only_topic=only_topic)

    t4 = time.time()
    print "Recommend_by_user_info recall time", t4 - t3

    info_logger.info("%s=recall_ids=%s===============uid=%s========", log_mark, '-'.join(recall_ids), uid)

    # 排序
    t5 = time.time()
    topn_ids_scores, title_dict, v_score_dict = rank(uid=uid, recall_ids=recall_ids, title_dict=title_dict,
                                                     solr_score_dict=score_dict,
                                                     u_vecs=u_vecs, u_weights=u_weights, u_tags=tags, u_cates=u_cates,
                                                     keep=num + 9)

    t6 = time.time()
    print "Recommend_by_user_info rank time", t6 - t5

    info_logger.info("%s=topn_ids_scores_len=%s================uid=%s========", log_mark, len(topn_ids_scores), uid)

    if len(topn_ids_scores) == 0:
        bad_return["status"] = "topn_ids_scores empty"  # 往往因为物料不足或者召回量过低导致
        return bad_return
    t7 = time.time()
    best = aftertreatment(topn_ids_scores, {}, special_population, num)
    t8 = time.time()
    print "Recommend_by_user_info aftertreatment time", t8 - t7

    if not best:
        bad_return["status"] = "no best"
        return bad_return

    # get threshhold
    if not test:
        threshhold = trigger_threshhold(trigger)
    else:
        threshhold = -9999999.9

    best_id, _ = best[0]
    best_score = v_score_dict[best_id]
    if best_score < threshhold:  # 最高分低于阈值
        bad_return["status"] = "best_score too low"
        return bad_return

    res = [[int(item[0].split('_')[1]), title_dict.get(item[0], u''), item[0].split('_')[0]] for item in
           best]  # [[111,title1,"news"],[222,title2],...]

    if not test:
        return {"res": res, "status": "succeed"}
    else:
        return {"res": res, "user_info": user_info, "topn_ids_scores": topn_ids_scores, "only_topic": only_topic,
                "status": "succeed", "v_score_dict": v_score_dict}


#########################################################################################
TEST_RETURN_Recommend_tags = {'words': [u"吃得太多", u"消化太好", u"还不运动", u"无药可医", u"病入膏肓"],
                              'plan': [{'url': "https://www.chunyuyisheng.com/pc/article/115415/", 'name': u"哦嘿"},
                                       {'url': "https://www.chunyuyisheng.com/pc/article/115281/", 'name': u"呀哈"}]}

BAD_TEST_RETURN_Recommend_tags = {'words': [], 'plan': []}


def Recommend_tags(uid):
    # ******************************************
    # if uid == -1:
    #     return BAD_TEST_RETURN_Recommend_tags
    # return TEST_RETURN_Recommend_tags
    # ******************************************
    # 取用户最后一个query
    # 返回 最相似疾病词*2，最相似症状词*1，最相似药品词*2（按顺序给出词），和 解决方案
    # 别忘了打log
    log_mark = "recommend_tag_tag_tag"
    info_logger.info(
        "%s===uid=%s====start==========", log_mark, uid
    )
    bad_return = BAD_TEST_RETURN_Recommend_tags
    # return TEST_RETURN_Recommend_tags
    if uid == -1:
        return bad_return
    last_query = get_user_last_query(uid)
    info_logger.info(
        "%s===uid=%s====last_query=%s=======", log_mark, uid, last_query
    )
    tags = get_similar_entities(query=last_query)
    plans = get_relation_plan(query=last_query)
    info_logger.info(
        "%s===tags=%s=======uid=%s=============", log_mark, '|||'.join(tags), uid
    )

    plans_string = '|||'.join([item['name'] for item in plans])
    info_logger.info(
        "%s===plans=%s=======uid=%s=============", log_mark, plans_string, uid
    )

    return {'words': tags, 'plan': plans}


#########################################################################################
TEST_RETURN_Recomend_topics = [1466, 1467, 1468]
BAD_TEST_RETURN_Recomend_topics = []


def Recommend_topics_kernel(uid, num, timestamp=None, test=False):
    if not test:
        bad_return = []
    else:
        bad_return = [], None, None
    log_mark = "recommend_topic_topic"
    # step 1 先从solr里找，看有没有被离线计算过，8ms左右
    if not test:
        solr_res = get_caled_user_topn_topics_yesterday0(uid)
        if solr_res is not None:
            info_logger.info(
                "%s===uid=%s====return==offline_data==topic_ids=%s=======", log_mark, uid, solr_res
            )
            print 'hehehehe', uid, solr_res
            return solr_res

    user_info0 = get_yesterday_user_info(uid, timestamp)
    if user_info0['last_event'] is None:
        info_logger.info(
            "%s==failed in no last event=uid=%s========", log_mark, uid
        )
        return bad_return
    # print json.dumps(user_info0)
    t1 = time.time()
    user_info = parse_user_info2(user_info0)
    t2 = time.time()
    print "Recommend_topics_kernel parse_user_info2 time", t2 - t1
    texts = user_info['texts']
    tags = user_info['tags']
    u_weights = user_info['weights']
    u_cates = user_info['cates']
    special_population = user_info['special_population']
    center = None
    trigger = user_info['trigger']
    u_vecs = user_info['vecs']

    if len(tags) == 0:
        info_logger.info(
            "%s==failed in no tags=uid=%s========", log_mark, uid
        )
        return bad_return
    t3 = time.time()
    recall_ids, title_dict, score_dict = recall(text=' '.join(texts), tags=tags,
                                                weights=u_weights,
                                                cates=u_cates,
                                                special_population=special_population,
                                                center=center, trigger_type=trigger,
                                                only_topic=True,
                                                yxjt=True)

    t4 = time.time()
    print "Recommend_topics_kernel recall time", t4 - t3

    info_logger.info((log_mark + "===recall_ids", recall_ids))
    t5 = time.time()
    topn_ids_scores, title_dict, v_score_dict = rank(uid=uid, recall_ids=recall_ids, title_dict=title_dict,
                                                     solr_score_dict=score_dict,
                                                     u_vecs=u_vecs, u_weights=u_weights, u_tags=tags, u_cates=u_cates,
                                                     keep=num)
    t6 = time.time()
    print "Recommend_topics_kernel parse_user_info2 rank", t6 - t5

    threshhold = trigger_threshhold(trigger)

    if test:
        return [int(item[0].split('_')[-1]) for item in topn_ids_scores if
                v_score_dict[item[0]] >= threshhold], user_info, v_score_dict
    return [int(item[0].split('_')[-1]) for item in topn_ids_scores if
            v_score_dict[item[0]] >= threshhold]


def Recommend_topics(uid, num, timestamp=None, test=False):
    # if uid == -1:
    #     return BAD_TEST_RETURN_Recomend_topics
    # return TEST_RETURN_Recomend_topics
    log_mark = "recommend_topic_topic"
    info_logger.info(
        "%s===uid=%s====start====num=%s=======", log_mark, uid, num
    )
    topic_ids = Recommend_topics_kernel(uid=uid,
                                        num=num,
                                        timestamp=timestamp,
                                        test=test)

    info_logger.info(
        "%s===uid=%s====return====topic_ids=%s=======", log_mark, uid, topic_ids
    )
    return topic_ids


#########################################################################################
BAD_TEST_RETURN_Recommend_plan = []
TEST_RETURN_Recommend_plan = [15, 16]


def Recommend_plan(uid, num):
    bad_return = BAD_TEST_RETURN_Recommend_plan
    # if not IS_ONLINE_WEB_SERVER:
    #     return TEST_RETURN_Recommend_plan
    log_mark = "recommend_plan"
    info_logger.info(
        "%s===uid=%s====start====num=%s=======", log_mark, uid, num
    )

    if uid == -1:
        return bad_return
    try:
        uid = str(uid)
    except:
        info_logger.info(
            "%s===uid=%s============uid unvalid=======", log_mark, uid
        )
        return bad_return

    res = get_relation_plan3(uid, num)
    systagids = res.get('ids', [])
    status = res['status']
    plan_ids = systagid_2_planid(systagids)
    plans_string = '|||'.join([str(item) for item in plan_ids])
    info_logger.info(
        "%s===plans=%s=======uid=%s======status=%s=======", log_mark, plans_string, uid, status
    )
    return plan_ids


#########################################################################################
BAD_TEST_RETURN_Recommend_plan = []
TEST_RETURN_Recommend_news = [977, 1008, 8698]


def Recommend_news(uid, num, solr_first=True, redis_second=True):
    bad_return = BAD_TEST_RETURN_Recommend_plan
    if not IS_ONLINE_WEB_SERVER:
        shuffle(TEST_RETURN_Recommend_news)
        return TEST_RETURN_Recommend_news[:num]
    log_mark = "recommend_news"
    info_logger.info(
        "%s===uid=%s====start====num=%s=======", log_mark, uid, num
    )

    if uid == -1:
        return bad_return
    try:
        uid = str(uid)
    except:
        info_logger.info(
            "%s===uid=%s============uid unvalid=======", log_mark, uid
        )
        return bad_return

    # 首先尝试去solr取结果
    if solr_first:
        solr_res = get_ramdom_topn_news_from_solr(uid, num)
        if solr_res:
            log_string = '|'.join([str(x) for x in solr_res])
            info_logger.info(
                "%s===uid=%s=====solr_newsids=%s============", log_mark, uid, log_string
            )
            return solr_res[:num]

    # 再尝试去redis里取结果
    if redis_second:

        redis_ids = get_random_topn_news_from_redis(uid, num)
        if redis_ids:
            log_string = '|'.join([str(x) for x in redis_ids])
            info_logger.info(
                "%s===uid=%s=====redis_newsids=%s============", log_mark, uid, log_string
            )
            return redis_ids[:num]

    res = recommend_news_kernel(uid=uid, test=False, num=num + 10)

    ids = res['ids']
    # 如果使用redis，则将计算结果存入redis，以便下次刷新时使用
    if redis_second:
        write_user_all_news_to_redis(uid, ids)

    # 随机选取num个文章
    ids = select_newsid(ids, num)

    if redis_second:
        write_user_showed_news_to_redis(uid, ids)

    log_string = '|'.join([str(x) for x in ids])
    info_logger.info(
        "%s===newsids=%s=======uid=%s=============", log_mark, log_string, uid
    )

    return ids[:num]


def test_recommend_news():
    import sys
    uid = sys.argv[1]
    ids = Recommend_news(uid, 2)
    print ids, type(ids)


if __name__ == '__main__':
    test_recommend_news()
