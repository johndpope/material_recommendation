# encoding=utf8


import re
import time
import numpy as np
from random import shuffle

from chunyu.utils.general.encoding_utils import ensure_unicode
from medweb_utils.utilities.log_utils import info_logger

from general_utils.db_utils import get_newsdb_handler, get_db_data_local_handler, get_medicaldb_handler
from general_utils.hbase_utils import user_half_year_newsids, user_half_year_topics
from general_utils.word2vec_utils import vecs_similarity, get_center, get_center_weighted, vecs_similarity2, \
    vecs_similarity3
from general_utils.solr_utils import get_candidate_news2, vec2string, get_news_tags_from_solr, search_by_tags, \
    more_topic_from_solr, more_news_from_solr, more_news_and_topic_from_solr, get_bodypart_word, \
    nat_get_title, nat_get_digest, nat_get_topic_score, nat_get_newstype

from general_utils.text_utils import qa_ask_info

from recommend.consts import CLINIC_NO_MAP

from rpc_services.medical_service_utils import get_medical_entities_info2, get_medical_entities_info2_cyseg
from rpc_services.search_api import get_news_from_bigsearch
from rpc_services.qa_promotion_api import get_special_population
from rpc_services.word2vec_api import get_vecs_weighted3, get_vec_dict_norm_ndarray_redis, \
    get_vec_list_norm_ndarray_redis
from rpc_services.problem_triage_api import second_clinic, first_clinic


##################################################################
# special population
def is_for_pregnant(text):
    # 用规则判断是不是备孕人群(刘慧珠)
    text = ensure_unicode(text)
    patterns = (u"想怀孕",
                u"尝试怀孕",
                u"怎么才能怀孕",
                u"能不能怀孕",
                u"能怀孕",
                u"可不可以怀孕",
                u"可以怀孕",
                u"备孕",
                u"影响怀孕",
                u"想要宝宝",
                u"要宝宝",
                u"要孩子",
                u"要小孩",
                u"想生宝宝",
                u"想生小孩",
                u"想生孩子",
                u"如何怀孩子",
                u"如何生孩子",
                u"怎样怀孩子",
                u"怎样生孩子",
                u"如何怀宝宝",
                u"如何生宝宝",
                u"怎样怀宝宝",
                u"怎样生宝宝",
                u"如何怀上孩子",
                u"怎样怀上孩子",
                u"如何怀上宝宝",
                u"如何怀上宝宝",
                )
    for p in patterns:
        if p in text:
            return True
    return False


POPULATION_TYPE_MAP = {
    # 将hbase里边转换成int
    "children": 0,
    "elder": 1,
    "pregnant_woman": 2,
    "lactating_women": 3,
    "for_pregnant": 4,  # 用规则优先判定
    "common_population_women": 5,
    "common_population_men": 6,
}

NEWS_TYPE_MAP = {
    # 日了狗的命名方式。。。
    "PY": 0,  # 辟谣
    "SH": 1,  # 生活
    "MALE": 2,  # 男性
    "LXXZ": 3,  # 两性
    "JS": 4,  # 减肥
    "NX": 5,  # 女性
    "MYBJ": 6,  # 母婴
    "MR": 7,  # 美容
    "YY": 8,  # 营养
    "QGXL": 9,  # 情感
    "AZ": 10,  # 癌症
    "TNB": 11,  # 糖尿病
}


def get_sex(text):
    text = ensure_unicode(text)
    if u"男" in text:
        return 2
    if u"女" in text:
        return 1
    return 0


def population_pros(my_population_type):
    '''
    规则
    1 孕妇、哺乳期妇女、儿童：首推母婴、美容、营养；不推男性
    2 老人：首推辟谣、癌症、糖尿病、营养
    3 男性（普通人群再用"男"）：不推女性、母婴
    4 女性（普通人群再用"女"）：不推男性、母婴
    5 备孕：首推母婴、营养
    6 儿童：首推母婴，营养。不推男性，女性，两性。
    （关于"首推"：刘慧珠：我觉得top5里面有首推文章就可以推了）
    不推的规则应该solr召回的时候做
    '''
    if my_population_type in (2, 3, 0):
        return [6, 7, 8]
    if my_population_type == 1:
        return [0, 8, 10, 11]
    if my_population_type == 4:
        return [6, 8]
    return []


def population_pros2(population_type):
    # 首推
    population_type = ensure_unicode(population_type)
    if population_type in (u"children", u"pregnant_woman", u"lactating_women"):
        return [u"MYBJ", u"MR", u"YY"]
    if population_type == u"elder":
        return [u"PY", u"YY", u"AZ", u"TNB"]
    if population_type == u"children":
        return [u"MYBJ", u"YY"]
    if population_type == u"for_pregnant":
        return [u"MYBJ", u"YY"]
    return []


def population_cons(my_population_type):
    '''
    不推的规则在这
    '''
    if my_population_type in (2, 3, 0):
        return [2]  # 孕妇、哺乳期妇女、儿童 不推男性
    if my_population_type == 6:
        return [5, 6]  # 男性不推女性、母婴
    if my_population_type == 5:
        return [2, 6]  # 女性不推男性、母婴
    return []


def population_cons2(population_type):
    if population_type is None:
        return []
    population_type = ensure_unicode(population_type)
    if population_type == u"children":
        return [u"MALE", u"NX", u"LXXZ"]
    if population_type == u"lactating_women":
        return [u"MALE"]
    if population_type == u"common_population_men":
        return [u"NX", u"MYBJ", u"LXXZ"]
    if population_type == u"common_population_women":
        return [u"MALE", u"MYBJ", u"LXXZ"]
    if population_type == u"common_population":
        return [u"MYBJ", u"LXXZ"]
    if population_type not in (u"pregnant_woman", u"lactating_women", u"children"):
        return [u"MYBJ"]
    return []


def population_limits(population_type):
    if population_type == u"pregnant_woman":
        return [u"MYBJ", u"YY", u"MR"]


def aftertreatment(sorted_score_dict, news_type, special_population, num=1):
    # 基于规则优先"推"的的后处理
    bad_return = None
    if len(sorted_score_dict) == 0:
        return bad_return
    pros = population_pros2(special_population)
    cons = population_cons2(special_population)
    good_score_list = []
    cnt = 0
    for unique_id, score in sorted_score_dict:

        material_type, id = unique_id.split('_')
        if material_type == "news":
            cnt += 1
            try:
                nt = news_type[id]
            except:
                # nt = get_db_data_local_handler().get_news_type(id)
                nt = nat_get_newstype(id)
            if nt in cons:
                continue
            if nt in pros and cnt <= 5 and num == 1:
                return [[unique_id, score]]
            good_score_list.append([unique_id, score])
        elif material_type == "topic":
            good_score_list.append([unique_id, score])

    try:
        return good_score_list[:num]
    except:
        return bad_return




def special_population_big(text):
    if is_for_pregnant(text):
        return u"for_pregnant", 1
    sp0 = get_special_population(text)
    sex = get_sex(text)
    if sp0 == u'common_population' and sex == 1:
        return u'common_population_women', 1
    if sp0 == u'common_population' and sex == 2:
        return u'common_population_men', 2
    return sp0, sex


######################################################


# def selecte_best_candidates2(solr_res, u_vecs, u_center_weighted, bad_ids, disease, keep=10):
#     score_dict = {}
#     news_type = {}
#     titles = set()
#     for item in solr_res:
#         newsid = int(item['id'].split('_')[-1])
#         title, _ = get_newsdb_handler().get_title_digest_by_nid(newsid)
#         if title in titles:
#             continue  # 去重
#         if newsid in bad_ids:
#             continue
#         titles.add(title)
#         n_tags = get_news_tags_from_solr(item['id'])
#         n_weights = [1.2 if x in disease else 1.0 for x in n_tags]
#         n_vecs, n_weights = get_vecs_weighted(n_tags, n_weights)
#
#         n_center_weighted = get_center_weighted(n_vecs, n_weights)
#         score = vecs_similarity(u_vecs, u_center_weighted, n_vecs, n_center_weighted)
#         score_dict[newsid] = score
#         news_type[newsid] = item.get("clinic_no", "other")
#     sorted_score_dict = sorted(score_dict.iteritems(), key=lambda x: x[1], reverse=True)
#     return sorted_score_dict[:keep], news_type


def recall_old(text, tags, weights, special_population, center, trigger_type, only_topic=False):
    # 先从大搜里取，若大搜的结果小于3等于个，去掉第一个，剩下的和从solr召回的合并排序
    # 若tags少于5个（太多了比较慢），solr召回时用content:*xx*召回100个, content不能找回可观数量的，再考虑使用向量召回
    # 若tags大于等于5个，solr召回时用content:*xx*召回100个, content不能找回可观数量的，再考虑使用向量召回

    # 去掉is_online（tid）=0的文章
    if len(tags) == 0 or center is None:
        return []

    if not only_topic:
        # news_recall = recall_news(text, tags, special_population, center, content_type="news")
        news_recall, news_title_dict = recall_news2(text, tags, weights, special_population)
    else:
        news_recall = set()
    topic_recall, topic_title_dict = recall_topic(text, tags, center, trigger_type)
    print "only_topic", only_topic
    print "news_recall", news_recall
    print "topic_recall", topic_recall
    return news_recall, topic_recall, topic_title_dict


def recall(text, tags, weights, cates, special_population, center, trigger_type, only_topic=False, yxjt=False):
    return recall_together(text, tags, weights, cates, special_population, trigger_type=trigger_type,
                           only_topic=only_topic, yxjt=yxjt)


def recall_together(text, tags, weights, cates, special_population,
                    trigger_type=None, only_topic=False, yxjt=False):
    # news and topic 一起召回
    news_cons = population_cons2(special_population)  # 不能在这些分类
    news_limits = population_limits(special_population)  # 必须在这些分类
    if trigger_type == "big_search":
        drug_words = [x for x in tags if (x in cates and cates[x] == 'DRUG_DESC')]
    else:
        drug_words = None
    rows = 25  # 这里太大可能导致文章多->访问word2vec次数多->超时的可能性变大
    if yxjt:  # 医学讲堂
        rows = 25  # 医学讲堂只处理标题所以这里可以取大一些
    res_ids, title_dict, score_dict = more_news_and_topic_from_solr(
        # text=text,
        text='',
        tags=tags,
        weights=weights,
        rows=rows,
        drug_words=drug_words,
        news_cons=news_cons,
        news_limits=news_limits,
        topic_only=only_topic
    )

    # 召回后，进行过滤
    res_ids1 = []
    all_titles = set()
    all_doctor_ids = set()
    for id in res_ids:
        print '--------======-----'
        print id
        # news and topic 标题去重
        title = title_dict.get(id, '')
        if title in all_titles:
            # 对所有标题去重
            continue
        all_titles.add(title)

        # 获取物料类型和真实id
        type, true_id = id.split('_')

        # 医学讲堂标题长度限制
        if type == 'topic' and yxjt:
            if len(title) < 8:
                continue

        # 对topic的医生id去重
        if type == 'topic':
            doctor_id = get_medicaldb_handler().get_topic_doctor_id(true_id)
            if doctor_id and doctor_id in all_doctor_ids:
                continue
            all_doctor_ids.add(doctor_id)

        # 规则过滤
        if not child_match(special_population, text, title_dict[id]):
            continue

        res_ids1.append(id)

    print "recall ids"
    for id in res_ids1:
        print id, score_dict[id], title_dict[id]

    return res_ids1, title_dict, score_dict


def recall_news2(text, tags, weights, special_population):
    # solr robot_news表里 title,title_tag,news_tag字段使用不同的 权重搜索
    # 构造solr query 的 fq
    cons = population_cons2(special_population)  # 不能在这些分类
    limits = population_limits(special_population)  # 必须在这些分类
    recall_news_ids, news_title_dict = more_news_from_solr(text=text, tags=tags, weights=weights,
                                                           rows=30, news_type_cons=cons,
                                                           news_type_limits=limits)
    return recall_news_ids, news_title_dict


def recall_news(text, tags, special_population, center, content_type="news"):
    # big_search recall
    # 已经不用辣
    big_search_newsids = [int(x) for x in get_news_from_bigsearch(text)]
    if len(big_search_newsids) > 3 or center is None:
        return big_search_newsids

    # solr recall
    cons = population_cons2(special_population)  # 不能在这些分类
    limits = population_limits(special_population)  # 必须在这些分类

    if len(tags) < 5:
        tags_newsids = [int(item.split('_')[-1]) for item in
                        search_by_tags(tags, content_type, cons=cons, limits=limits, rows=30)]


    else:
        tags_newsids = [int(item.split('_')[-1]) for item in
                        search_by_tags(tags, content_type, cons=cons, limits=limits, rows=30)]

    if len(tags_newsids) <= 5:
        # 词召回太少就用向量召回##########可以修改的地方
        center_string = vec2string(center)
        center_newsids = [int(item['id'].split('_')[-1]) for item in
                          get_candidate_news2(center_string, content_type, rows=20, cons=cons)]
    else:
        center_newsids = []
    return list(set(center_newsids) | set(tags_newsids) | set(big_search_newsids))


def recall_topic(text, tags, weights, trigger_type):
    # 医生话题召回
    # 用text和tags分别从大搜more_topic召回50个，去掉没有分数的（总共13000左右），科室不相符的
    # 已经不用辣

    bad_return = [], {}
    if len(tags) == 0:
        # 没有医学实体其实也可以召回一些。。。
        print "no tags when recall topic"
        return bad_return
    # user_clinic = str(first_clinic(text))  # 一级科室
    recall_topic_ids, topic_title_dict = more_topic_from_solr(text, tags, weights, 30)

    title_dict = {}
    topic_ids = set()
    print "topic num before filter", len(topic_title_dict)
    print "~~~~~~~~~~"
    print "trigger_type", trigger_type

    for id in recall_topic_ids:
        print "-------topic id=%s---" % id
        if trigger_type in ("big_search", "free_problem_create"):
            # 大搜 qa 使用 特殊人群标签 限制 宝宝-非宝宝
            if child_match(text, topic_title_dict[id]):
                topic_ids.add(id)
                title_dict[id] = topic_title_dict[id]

    print "topic num after filter", len(topic_ids)
    return list(topic_ids), title_dict


# def weighted_news_tags2(news_id):
#     #### 不要了
#     weight_list = [2.0, 1.0]  # 标题和摘要的权重
#     title = get_db_data_local_handler().get_news_title(news_id)
#     digest = get_db_data_local_handler().get_news_title(news_id)
#
#     empty_title = False  # 标题没有实体词的文章
#     words, weights, cates, counts = get_medical_entities_info([title, digest], weight_list)
#     if len(words) == 0:
#         return False, None, None, None, None, None
#     if counts[0] == 0:
#         # title里没有实体词
#         empty_title = True
#     title_tags = words[:counts[0]]
#     return True, words, weights, cates, empty_title, title_tags


def weighted_news_tags3(title, digest):
    weight_list = [3.0, 1.0]
    empty_title = False  # 标题没有实体词的文章
    # words, weights, cates, counts = get_medical_entities_info([title, digest], weight_list)
    words, weights, cates, counts, entities, entity_counts = get_medical_entities_info2_cyseg([title, digest],
                                                                                              weight_list,
                                                                                              weights_is_dict=True)

    if len(words) == 0:
        return False, None, None, None, None, None
    if entity_counts[0] == 0:
        # title里没有词
        empty_title = True
    title_tags = words[:counts[0]]
    return True, words, weights, cates, empty_title, title_tags


def user_news_tag_hard_match(u_tags, u_cates, n_tags_title):
    # BS和QA里面身体部位和症状硬匹配,
    # 已经不用辣
    if "BODYPART_DESC" not in u_cates.values():
        return True
    u_bodyparts = [x for x in u_tags if u_cates.get(x, "") == "BODYPART_DESC"]
    for x in u_bodyparts:
        if x in n_tags_title:
            return True
    return False


# def user_news_tag_bodypart_match(u_bp_words, n_text):
#     # user tags 中出现的 bodypart words，需要跟ntext中的bodypart words中至少存在一个匹配（相同或余弦相似度大于0.4)
#     # 已经不用辣
#     if not u_bp_words:
#         return 1.0
#     n_bp_words = get_bodypart_word(n_text)
#     if not n_bp_words:
#         return 0.8
#     # user news(topic)都有bodypart word
#     # 若交集不为空
#     if len(set(n_bp_words) & set(u_bp_words)) > 0:
#         return 1.3
#     # 若交集为空
#     for x in u_bp_words:
#         for y in n_bp_words:
#             v_x = get_vec(x)
#             v_y = get_vec(y)
#             if v_x is not None and v_y is not None:
#                 cossim = np.dot(v_x, v_y)
#                 if cossim > 0.4:
#                     return 1.1
#     return 0.3  # 物料中存在bodypart word但与u_bp_words不匹配


def user_news_tag_bodypart_match2(u_bp_words, ntags):
    if not u_bp_words:
        return 1.0
    n_bp_words = find_bdpart(ntags)
    print "n_bp_words", '-'.join(n_bp_words)
    if not n_bp_words:
        return 0.8
    # user news(topic)都有bodypart word
    # 若交集不为空
    if len(set(n_bp_words) & set(u_bp_words)) > 0:
        return 1.3
    # 若交集为空
    u_bp_words_vecs_list = get_vec_list_norm_ndarray_redis(u_bp_words)
    n_bp_words_vecs_list = get_vec_list_norm_ndarray_redis(n_bp_words)
    for x in u_bp_words_vecs_list:
        for y in n_bp_words_vecs_list:
            if np.dot(x, y) > 0.4:
                return 1.1
    return 0.3  # 物料中存在bodypart word但与u_bp_words不匹配


def rank(uid, recall_ids, title_dict, solr_score_dict, u_vecs, u_weights, u_tags, u_cates, keep=10):
    # 增加标题摘要实体词的权重，减小仅从正文提取到实体词文章的分数
    # 去掉标题摘要正文只有一个实体词的文章    后处理
    uid = int(uid)
    score_dict = {}
    v_score_dict = {}
    # title_dict = {}
    u_bp_words = get_bodypart_word('-'.join(u_tags))

    print "u_bp_words", '+'.join(u_bp_words)
    news_ids = [x for x in recall_ids if x.startswith("news")]
    topic_ids = [x for x in recall_ids if x.startswith("topic")]
    # calculate news similarities

    rank_news(news_ids=news_ids,
              solr_score_dict=solr_score_dict,
              score_dict=score_dict,
              v_score_dict=v_score_dict,
              uid=uid,
              u_vecs=u_vecs,
              u_weights=u_weights,
              u_bp_words=u_bp_words)

    # calculate topic similarities
    rank_topic(topic_ids=topic_ids,
               title_dict=title_dict,
               score_dict=score_dict,
               v_score_dict=v_score_dict,
               solr_score_dict=solr_score_dict,
               uid=uid, u_vecs=u_vecs, u_weights=u_weights,
               u_bp_words=u_bp_words)

    sorted_id_score = sorted(score_dict.iteritems(), key=lambda x: x[1], reverse=True)

    return sorted_id_score[:keep], title_dict, v_score_dict


def rank_topic(topic_ids, solr_score_dict, score_dict, v_score_dict, title_dict, uid, u_vecs, u_weights, u_bp_words):
    bad_ids = user_half_year_topics(uid)
    all_n_tangs_cnt = 0
    all_tags_vecs = {}  # 本次rank所有词向量
    cnt = 0
    for id in topic_ids:
        if cnt > 10:
            continue
        print "-----------"
        print id
        _, true_id = id.split('_')
        if int(true_id) in bad_ids:
            continue
        title = title_dict.get(id, "")
        print title
        t1 = time.time()

        is_good_article, n_tags, n_weights, n_cates, empty_title, title_tags = weighted_news_tags3(title, "")
        # n_weights is a dict
        t2 = time.time()

        try:
            all_n_tangs_cnt += len(n_tags)
        except:
            pass
        print "weighted_news_tags3 time", t2 - t1

        if not is_good_article or len(n_tags) == 0:
            continue

        # n_vecs, n_keep = get_vecs_weighted3(n_tags)
        new_tags = set(n_tags) - set(all_tags_vecs.keys())
        new_tags_vecs_dict = get_vec_dict_norm_ndarray_redis(new_tags)

        all_tags_vecs.update(new_tags_vecs_dict)

        t3 = time.time()
        print "get_vec_list_norm_ndarray time", t3 - t2
        # n_weights = [n_weights[i] for i in n_keep]
        # score = vecs_similarity2(u_vecs, u_weights, n_vecs, n_weights)
        score = vecs_similarity3(u_vecs=u_vecs,
                                 u_weights=u_weights,
                                 n_vecs_dict=all_tags_vecs,
                                 n_weights_dict=n_weights,
                                 n_tags=n_tags,
                                 )
        # 身体部位匹配度分数
        bp_score = user_news_tag_bodypart_match2(u_bp_words, title_tags)
        print 'bp_score', bp_score
        print "score0", score
        print "ntags", '-'.join(n_tags)

        if len(title_tags) == 1 and title_tags[0] in (u"疼", u"痛", u"疼痛"):
            score *= 0.4

        t4 = time.time()

        print "vecs_similarity2 time", t4 - t3
        # print id,score

        if score <= 0.1:
            continue
        topic_score = nat_get_topic_score(id)
        print "topic_score", topic_score
        if abs(topic_score - 0.0) < 0.1:
            continue
        topic_score = adjust_topic_score2(topic_score)  # 调整topic_score到0-1之间使得topic总分数也跟news一样

        solr_score = solr_score_dict[id]

        v_score_dict[id] = score * topic_score * bp_score
        score_dict[id] = (0.9 * score + 0.1 * solr_score) * topic_score * bp_score
        cnt += 1
    print "all_n_tangs_cnt topic", all_n_tangs_cnt


def rank_news(news_ids, solr_score_dict, score_dict, v_score_dict, uid, u_vecs, u_weights, u_bp_words):
    t1 = time.time()
    bad_ids = user_half_year_newsids(uid)  # int
    t2 = time.time()
    print "user_half_year_newsids time", t2 - t1
    titles = set()
    cnt = 0
    all_n_tangs_cnt = 0
    all_tags_vecs = {}  # tag:vec
    for id in news_ids:
        print '='*20,id
        if cnt >= 10:
            break
        _, true_id = id.split('_')
        if int(true_id) in bad_ids:
            continue
        # 标题去重
        title = nat_get_title(id)

        if title in titles:
            if len(title) > 0:
                # 防止访问数据库出错导致推送失败
                continue
        titles.add(title)

        digest = nat_get_digest(id)
        # 提取文章tags
        t3 = time.time()
        is_good_article, n_tags, n_weights, n_cates, empty_title, title_tags = weighted_news_tags3(title, digest)
        # n_weights is a dict
        t4 = time.time()
        print "weighted_news_tags3 time", id, t4 - t3
        try:
            all_n_tangs_cnt += len(n_tags)
        except:
            pass
        # 去掉没tag的文章
        if not is_good_article or len(n_tags) <= 1:
            continue

        t5 = time.time()
        print "user_news_tag_hard_match time", t5 - t4

        new_tags = set(n_tags) - set(all_tags_vecs.keys())
        new_tags_vecs_dict = get_vec_dict_norm_ndarray_redis(new_tags)

        all_tags_vecs.update(new_tags_vecs_dict)  # 更新到all_tags_vecs

        t6 = time.time()
        print "get_vecs_weighted3 time", t6 - t5

        # 计算相似度
        # score = vecs_similarity2(u_vecs, u_weights, n_vecs, n_weights)
        score = vecs_similarity3(u_vecs=u_vecs,
                                 u_weights=u_weights,
                                 n_vecs_dict=all_tags_vecs,
                                 n_weights_dict=n_weights,
                                 n_tags=n_tags)
        # 身体部位匹配度分数
        bp_score = user_news_tag_bodypart_match2(u_bp_words, title_tags)
        print 'bp_score', bp_score
        print 'score0', score
        print 'ntags', '-'.join(n_tags)
        t7 = time.time()
        print "vecs_similarity2 time", t7 - t6
        if empty_title:
            score *= 0.5  # 标题没有实体词的文章降权
        solr_score = solr_score_dict[id]

        v_score_dict[id] = score * bp_score
        score_dict[id] = (0.7 * score + 0.3 * solr_score) * bp_score
        cnt += 1
    print "all_n_tangs_cnt news", all_n_tangs_cnt


def trigger_threshhold(trigger):
    if trigger == "big_search":
        return 0.2
    if trigger == "free_problem_create":
        return 0.35
    return 0.2


def get_all_text(user_info):
    all_text = []
    for key in user_info:
        if key in ("free_problem_create", "big_search"):
            all_text.extend(user_info[key])
    return all_text


def one_user_last_qa_info(pid):
    # 从数据库ask_problem表里把ask取出来，整理成hbase_utils里cy_time_event_one_user_kernel输出的格式
    info = {"last_event": None,
            "last_event_time": 0}

    text = get_medicaldb_handler().get_ask_by_pid(pid)
    sex = ''
    age = ''
    # info_logger.info("qa text %s", text)
    # text = u"感冒发烧了吃什么药好，二甲双胍可以吃吗 肺气肿 怀孕 糖尿病（男，1岁）"#############
    info["last_event"] = ["free_problem_create", [text, sex, age]]
    return info


def parse_user_info(user_info):
    last_event = user_info['last_event']
    info_logger.info("last_event %s", str(last_event))
    if last_event is None:
        return None
    timestamp = user_info['last_event_time']
    # output = {}  # trigger,timestamp,tags,vecs,weights,center,texts,sex,age,special_population
    if last_event[0] == 'big_search':
        texts = [item[0] for item in user_info['big_search']]
        text = ' '.join(texts).strip()

        # tags, weights, cates, _ = get_medical_entities_info([text], [1.0], 1.0)
        # 改为不仅仅使用医学实体词
        tags, weights, cates, counts, entities, entity_counts = get_medical_entities_info2_cyseg(
            text_list=[text],
            weight_list=[1.0],
            weights_is_dict=False  # weights以list形式返回
        )

        vecs, keep_indices = get_vecs_weighted3(tags)
        weights = [weights[i] for i in keep_indices]
        tags = [tags[i] for i in keep_indices]  # 去掉没有向量的tag
        entities = [x for x in entities if x in tags]  # 去掉没有向量的实体词

        center = get_center(vecs)
        special_population, sex = special_population_big(text)
        age = None
        output = {
            "trigger": 'big_search',
            "timestamp": timestamp,  # 这个没有用
            "tags": tags,
            "vecs": vecs,
            "entities": entities,
            "weights": weights,
            "cates": cates,
            "center": center,
            "texts": texts,
            "sex": sex,
            "age": age,
            "special_population": special_population,
        }

    elif last_event[0] == 'free_problem_create':
        text, sex, age = last_event[1]
        if sex:
            s_text = text + ' （%s，%s）' % (sex, age)
        else:
            s_text = text
            text, sex, age = qa_ask_info(text)
        special_population, sex = special_population_big(s_text)
        print special_population

        tags, weights, cates, counts, entities, entity_counts = get_medical_entities_info2_cyseg(
            text_list=[text],
            weight_list=[1.0],
            weights_is_dict=False  # weights以list形式返回
        )

        vecs, keep_indices = get_vecs_weighted3(tags)
        weights = [weights[i] for i in keep_indices]
        # cates is a dict
        tags = [tags[i] for i in keep_indices]  # 去掉没有向量的tag
        entities = [x for x in entities if x in tags]  # 去掉没有向量的实体词

        info_logger.info("len vecs %s", len(vecs))
        center = get_center(vecs)
        output = {
            "trigger": 'free_problem_create',
            "timestamp": timestamp,
            "tags": tags,
            "entities": entities,
            "vecs": vecs,
            "weights": weights,
            "cates": cates,
            "center": center,
            "texts": [text],
            "sex": sex,
            "age": age,
            "special_population": special_population,
        }
    else:
        print "output is None"
        output = None

    return output


def is_valid_user_info(user_info):
    if len(user_info['tags']) == 0 or len(user_info['vecs']) == 0:
        info_logger.info("=failed no tags=======")
        return False
    return True


def filter_user_info(user_info):
    # return True or False
    # 20171102第三次改进
    # 用户tag，单个词，这个词长度=1 or 是科室词 or 是身体部位词 or 药品词: False
    # tags,vecs 长度为0 ,center is None : False
    # 只有 "疼痛"：False
    # 20171103 qa 第四次改进
    # 触发事件为qa 且 仅有身体部位词 + 指定症状词（bad_word），不推
    # 触发事件为qa 且 仅有身体部位词 + 药品词，不推
    # 触发事件为qa 且 仅有1个或多个指定症状词，不推

    BAD_WORD = (u"疼痛",
                u"出血",
                u"硬块",
                u"很痛",
                u"非常痛",
                u"十分痛",
                u"疼",
                u"痛",
                u"阵痛",
                u"恶心",
                u"不适",
                u"红肿",
                u"感染",
                u"紧张",
                u"红",
                u"流血",
                u"头晕",
                u"不舒服",
                )

    if len(user_info['entities']) == 1:
        the_tag = user_info['entities'][0]
        if len(the_tag) <= 1 or user_info['cates'].values()[0] in (
                "BODYPART_DESC", "CLINIC_DESC", "DRUG_DESC") or the_tag in BAD_WORD:
            return False

    trigger = user_info['trigger']
    cates = user_info['cates']
    distinct_cates = set(cates.values())
    entities = user_info['entities']
    distinct_tags = set(entities)

    if trigger in ("free_problem_create", "big_search"):  # qa触发事件

        if len(distinct_cates) == 1 and "BODYPART_DESC" in distinct_cates:
            return False

        if distinct_tags == set([u"怀孕"]) or distinct_tags == set([u"流产"]) or distinct_tags == set([u"怀孕", u"流产"]):
            return False

        if len(distinct_cates) == 2 and "BODYPART_DESC" in distinct_cates:
            if "DRUG_DESC" in distinct_cates:  # 另一类是药品词:
                return False
            elif "SYMPTOM_DESC" in distinct_cates:
                good_symptom = []

                for i in range(len(entities)):
                    the_tag = entities[i]
                    cate = cates[the_tag]
                    if cate == "BODYPART_DESC":
                        continue

                    if the_tag not in BAD_WORD:
                        good_symptom.append(the_tag)
                if len(good_symptom) > 0:
                    return True
                else:
                    return False
        else:
            # 全是指定症状词，不推
            good_word = []
            for the_tag in entities:
                if the_tag not in BAD_WORD:
                    good_word.append(the_tag)
            if len(good_word) > 0:
                return True
            else:
                return False

    return True


LONELY_FIRST_CLINIC_NO = (
    # 没有二级科室的一级科室
    1,  # 妇科
    '1',
    6,  # 营养科
    '6',
    8,  # 男科
    '8',
    13,  # 口腔颌面科
    '13',
    15,  # 眼科
    '15',
    16,  # 整形美容科
    '16',
    21,  # 产科
    '21',
    19,  # 基因检测科
    '19',
)


def get_unique_clinic_no_2(topic_id):
    # 获取医生话题的科室信息
    clinic_no, second_class_clinic_no = get_medicaldb_handler().get_topic_clinic_no(topic_id)
    if clinic_no and clinic_no in LONELY_FIRST_CLINIC_NO:
        return str(clinic_no)
    if second_class_clinic_no:
        return str(second_class_clinic_no)
    return None


def get_unique_clinic_no_1(topic_id):
    # 获取医生话题的一级科室信息
    clinic_no, second_class_clinic_no = get_medicaldb_handler().get_topic_clinic_no(topic_id)
    if clinic_no and clinic_no in LONELY_FIRST_CLINIC_NO:
        return str(clinic_no)
    if second_class_clinic_no:
        return map_second_clinic_2_first(second_class_clinic_no)
    return None


def get_text_clinic_no(text):
    # 获取文本的一级科室信息
    return first_clinic(text)


def child_match(sp1, text1, text2):
    # 获取两个文本的特殊人群标签，都是宝宝或者都是非宝宝return True,否则return False
    # _, sex1 = special_population_big(text1)
    sp2, sex2 = special_population_big(text2)
    print "sp1", sp1
    print "sp2", sp2
    return (sp1 == "children") == (sp2 == "children") and sex_match(sp1, sp2) and pregnant_match(sp1, sp2)


def sex_match(sp1, sp2):
    return not (sp1 == "common_population_women" and sp2 == "common_population_men") or \
           (sp1 == "common_population_men" and sp2 == "common_population_women")


def pregnant_match(sp1, sp2):
    pregnant = ("pregnant_woman", "lactating_women")
    like_pregnant = ("pregnant_woman", "for_pregnant", "lactating_women")
    if (sp1 in pregnant and sp2 not in like_pregnant) or (sp1 not in like_pregnant and sp2 in pregnant):
        return False
    return True


def map_second_clinic_2_first(second_class_clinic_no):
    second_class_clinic_no = str(second_class_clinic_no)
    return CLINIC_NO_MAP.get(second_class_clinic_no, None)


def adjust_topic_score(o_score):
    # 最小0，最大19.5，如果想提高topic对news的分数，就用凹函数；反之用凸函数（convex）
    return -o_score * o_score / (3 * 19.5 * 19.5) + 4.0 * o_score / (3 * 19.5)


def adjust_topic_score2(o_score):
    # 90分以下线性，以上为1的分段函数
    if o_score >= 9.0:
        return 1.0
    return o_score / 9.0


def find_bdpart(tags, max_len=4):
    cutups = set()
    tags = [ensure_unicode(x) for x in tags]
    for tag in tags:
        for begin_index in range(len(tag)):
            l = len(tag)
            for w_size in range(0, l - begin_index):
                end_index = begin_index + w_size + 1
                if w_size > max_len:
                    break
                w = tag[begin_index:end_index]
                if get_db_data_local_handler().is_in_bodypart(w):
                    cutups.add(w)
    return cutups


def weights_normalization(weights):
    s = sum(weights)
    return [x / s for x in weights]


def weight_dict_normalization(weight_dict):
    '''
    将最大值设置为1，其余按照比例线性变化
    :param weight_dict:
    :return:
    '''
    m = float(max(weight_dict.values()))
    for key in weight_dict:
        weight_dict[key] /= m


def test():
    tags = u"嗓子发痒眉毛-痰手鼻骨右支-大腿发黄-眼睛-嘴-药好-".split('-')
    t1 = time.time()
    cutups = find_bdpart(tags)
    t2 = time.time()
    print '-'.join(cutups)
    print t2 - t1


if __name__ == '__main__':
    test()
