# encoding=utf8

import sys
import time
import json
from collections import defaultdict
from random import shuffle

from chunyu.utils.general.encoding_utils import ensure_unicode
from medweb_utils.utilities.log_utils import info_logger

from recommend.app_config import RECOMMED_TAGS_KEEP_NUM, RECOMMED_TAGS_ORDER
from recommend.manager.data_helper import special_population_big
from general_utils.hbase_utils import user_last_query, get_qa_texts_by_pid, get_user_recent_views, \
    get_news_id_from_cy_event2, get_user_query2, get_user_query3, get_qa_texts_by_pids
from general_utils.db_utils import get_db_data_local_handler, get_medicaldb_handler
from general_utils.time_utils import timestamp2datetime, ensure_second_timestamp, datetime_str2timestamp
from general_utils.solr_utils import get_cy_event_row_key_news, get_cy_event_row_key_topic, \
    nat_get_title, nat_get_digest, user_last_pids
from general_utils.text_utils import is_baby_text

from rpc_services.word2vec_api import get_similar, get_similar_redis
from rpc_services.medical_service_utils import get_entities_sp, get_entities_sp_cyseg

####################PM-RULES####################################
BAD_WORDS = [
    u"胃",
    u"指",
    u"钼",
    u"舌",
    u"头",
    u"肋",
    u"鼻",
    u"眼",
    u"痛",
    u"便",
    u"背",
    u"喉",
    u"脑",
    u"癌症",
    u"腭",
    u"足",
    u"瘿",
    u"尿",
    u"红",
    u"脊",
    u"聋",
    u"腮",
    u"肾",
    u"脚",
    u"骨",
    u"涕",
    u"疼",
    u"肠",
    u"钒",
    u"胆",
    u"痱",
    u"哕",
    u"痰",
    u"腰",
    u"颌",
    u"痧",
    u"趾",
    u"肌",
    u"腹",
    u"肺",
    u"盲",
    u"血",
    u"胰",
    u"脓",
    u"胸",
    u"肝",
    u"齿",
    u"额",
    u"脐",
    u"脾",
    u"腕",
    u"耳",
    u"痫",
    u"唇",
    u"鼻",
    u"痔",
    u"鄂",
    u"癣",
    u"心",
    u"痹",
    u"口",
    u"疝",
    u"痣",
    u"肛",
    u"肩",
    u"腿",
    u"胀",
    u"疣",
    u"啘",
    u"拳",
    u"手",
    u"肢",
    u"面",
    u"腱",
    u"膝",
    u"牙",
    u"痈",
    u"咽",
    u"掌",
    u"颈",
    u"疖",
    u"带血",
    u"瘦子",
    u"瘦弱",
    u"剧痒",
    u"剧痛",
    u"发冷",
    u"发凉",
    u"出汗",
    u"很痛",
    u"很瘦",
    u"很烫",
    u"发痒",
    u"发烫",
    u"很胖",
    u"怕冷",
    u"偏瘦",
    u"很疼",
    u"胖子",
    u"痛感",
    u"疼痛",
    u"肝有问题",
    u"肾有问题",
    u"肺有问题",
    u"肠胃有问题",
    u"小肠有问题",
    u"淋巴有问题",
    u"食道有问题",
    u"关节有问题",
    u"鼻窦有问题",
    u"阑尾有问题",
    u"直肠有问题",
    u"脊椎有问题",
    u"腰椎有问题",
    u"血压有问题",
    u"膀胱有问题",
    u"肺没有问题",
    u"胆囊有问题",
    u"脑袋有问题",
    u"卵巢有问题",
    u"小便有问题",
    u"尿路有问题",
    u"视力有问题",
    u"肾脏有问题",
    u"宫颈有问题",
    u"气管有问题",
    u"耳朵有问题",
    u"肾没有问题",
    u"听力有问题",
    u"心脏没有问题",
    u"输精管有问题",
    u"听力没有问题",
    u"前列腺有问题",
    u"甲状腺有问题",
    u"脊椎没有问题",
    u"输卵管有问题",
]

QUERY_CANCER_WORDS = [
    u"癌",
    u"瘤",
    u"恶化",
    u"恶变",
    u"硬化",
    u"恶性",
    u"未分化",
    u"低分化",
]

special_population_systag_id_limit = {
    # systag_id:sp 必须是特定的sp才能推这个systag_id
    24: "children",  # paediatrics
    48: "children",  # baby_diarrhea
    66: "children",  # baby_character
    69: "children",  # baby_expectorant
    72: "children",  # baby_growth
    78: "children",  # breast_milk"
    84: "children",  # baby_allergies
    87: "children",  # early_childhood
    111: "children",  # baby_cough
}

baby_systag_ids = [
    111, 24, 48, 66, 69, 72, 75, 84, 87,

]


def filter_systagid(systag_id, sp):
    systag_id = int(systag_id)
    return not (systag_id in special_population_systag_id_limit and sp != special_population_systag_id_limit[systag_id])


def is_cancer_text(text):
    for x in QUERY_CANCER_WORDS:
        if x in text:
            return True
    return False


############################################

KEEP_CATE = [
    # 没有bodypart
    "SYMPTOM_DESC",
    "DRUG_DESC",
    "DISEASE_DESC",
    "OPERATION_DESC",
    "CHECKUP_DESC",
    "CLINIC_DESC",
]


def get_user_last_query(uid):
    bad_return = ''
    try:
        uid = int(uid)
    except:
        return bad_return

    return user_last_query(uid)


def get_recall_num(len_entities):
    return int(50 / len_entities)


def get_similar_entities(query):
    # 先分词，再召回一定数量相近词，在里边找出
    bad_return = []
    # 分词
    t1 = time.time()
    entities, other_entities, relation_drug, other_relation_drug = get_entities_sp_cyseg(query, keep_cate=KEEP_CATE)
    # print 'entities', '-'.join(entities)
    # print 'other_entities', '-'.join(other_entities)
    # print 'relation_drug', '-'.join(relation_drug)
    # print 'other_relation_drug', '-'.join(other_relation_drug)
    # entities = get_entities(query, False,keep_cate=KEEP_CATE)
    t2 = time.time()
    # print "get_entities time", t2 - t1

    if (not entities) and (not other_entities):
        return bad_return

    if not entities:
        entities = other_entities
        relation_drug = other_relation_drug

    is_cancer_query = is_cancer_text(query)
    t3 = time.time()
    # print "query is_cancer_text time", t3 - t2
    # 每个实体词用get_similar召回10个(有待商榷，是固定数据，还是根据entities的数量变化）similar words
    similar_word_score_dict = {}
    l = len(entities)
    for entity in entities:
        # print "entity", entity
        # e_cate = get_db_data_local_handler().get_entity_cate(entity) not in RECOMMED_TAGS_KEEP_NUM
        for similar_w, score in get_similar_redis(entity, get_recall_num(l)):
            if similar_w in BAD_WORDS:
                continue
            if (not is_cancer_query) and is_cancer_text(similar_w):
                # 若query没出现癌，过滤癌类词
                continue
            if score > similar_word_score_dict.get(similar_w, 0.0):
                similar_word_score_dict[similar_w] = score

    t4 = time.time()
    print "get similar time", t4 - t3
    # 对相似词排序
    sorted_similarword_score = sorted(similar_word_score_dict.iteritems(), key=lambda x: x[1], reverse=True)
    t5 = time.time()
    print "sort time", t5 - t4
    # 选出一定数量的相似词
    selected_similar_words = defaultdict(list)
    for w, s in sorted_similarword_score:
        # 可以限制一下分数不低于阈值
        w_cate = get_db_data_local_handler().get_entity_cate(w)
        if w_cate not in RECOMMED_TAGS_ORDER:  # 不是这些类的词，不要
            continue
        if len(selected_similar_words[w_cate]) >= RECOMMED_TAGS_KEEP_NUM[w_cate]:  # 某类的词数够了，不要再加入了
            continue
        selected_similar_words[w_cate].append(w)
    t6 = time.time()
    print "select time", t6 - t5
    if len(selected_similar_words['drug']) == 0:
        # 若similar word中没有药品词，用医学实体库里存的relation_drug
        selected_similar_words['drug'] = list(relation_drug)[:2]
    # 将选出的相似词连接起来
    res = []
    for x in RECOMMED_TAGS_ORDER:
        # print x, ' '.join(selected_similar_words[x])
        res.extend(selected_similar_words[x])

    return res


def get_relation_plan(query, ):
    '''
    先找出可能的systag_ids，再投票取前2
    '''
    # print 'query', query

    systag_id_dict, cnt_dict = find_systag_keywords(query)

    sp, _ = special_population_big(query)
    # vote
    id_score = defaultdict(int)
    for keyword in systag_id_dict:
        weight = cnt_dict[keyword]
        for id in systag_id_dict[keyword]:
            id_score[id] += weight
    sorted_id_score = sorted(id_score.iteritems(), key=lambda x: x[1], reverse=True)
    selected_id = []
    for id, score in sorted_id_score:
        if not filter_systagid(id, sp):
            # pm的规则
            continue
        selected_id.append(id)
        if len(selected_id) >= 2:
            break
    # print "selected_id", selected_id
    selected_plan = []
    for id in selected_id:
        plan = get_db_data_local_handler().get_systag_relation_plan(id)
        selected_plan.extend(plan)

    shuffle(selected_plan)

    if len(selected_plan) > 1:
        return selected_plan[:2]
    return selected_plan


def get_relation_plan2(uid, num, test=False):
    '''
    step1 qa bs search_doctor 的 text 中匹配
    step2 阅读过文章、话题的title，digest中匹配
    step3 找医生科室匹配
    :param uid:
    :param test:
    :return:
    '''
    t1 = time.time()
    query, clinic_nos = get_user_query2(uid, num=5)  # 取10个搜索
    t2 = time.time()
    print "get_user_query2 time", t2 - t1
    qa = get_user_qa_content_smart(uid, num=2)  # 取10个qa
    t3 = time.time()
    print "get_user_qa_content_smart time", t3 - t2

    # step1
    step1_selected_plan, step1_systag_id_dict = get_relation_plan2_step1(' '.join(query + qa), num)
    t4 = time.time()
    print "get_relation_plan2_step1 time", t4 - t3
    if step1_selected_plan:
        res = {
            'plan': step1_selected_plan,
            'status': 1
        }

        if test:
            res['systag_id_dict'] = step1_systag_id_dict
        return res

    # step2
    view_texts = user_view_action_texts(uid, num=5)
    t5 = time.time()
    print "user_view_action_texts time", t5 - t4
    step2_selected_plan, step2_systag_id_dict = get_relation_plan2_step1(' '.join(view_texts), num)
    t6 = time.time()
    print "get_relation_plan2_step1 time", t6 - t5

    if step2_selected_plan:
        res = {
            'plan': step2_selected_plan,
            'status': 2
        }

        if test:
            res['systag_id_dict'] = step2_systag_id_dict
        return res

    # step3
    step3_selected_plan = get_relation_plan2_step3(clinic_nos, num)
    t7 = time.time()
    print "get_relation_plan2_step3 time", t7 - t6
    if step3_selected_plan:
        res = {
            'plan': step3_selected_plan,
            'status': 3
        }
        if test:
            res['clinic_no'] = clinic_nos

        return res

    return {'plan': [], 'status': 0}


def get_relation_plan2_step1(text, num, is_baby=False):
    '''
    text来源于qa，大搜，找医生；

    '''
    print "len text", len(text)
    # 召回
    systag_id_dict, cnt_dict = find_systag_keywords_extend(text)
    id_score = defaultdict(int)
    print 'is_baby', is_baby
    for keyword in systag_id_dict:
        weight = cnt_dict[keyword]
        for id in systag_id_dict[keyword]:
            if not is_baby and id in baby_systag_ids:
                continue
            id_score[id] += weight

    # 排序
    sorted_id_score = sorted(id_score.iteritems(), key=lambda x: x[1], reverse=True)
    selected_id = [item[0] for item in sorted_id_score]
    # selected_plan = []
    # for id, score in sorted_id_score:
    #     plan = get_db_data_local_handler().get_systag_relation_plan(id)
    #     selected_plan.extend(plan)

    # return selected_plan, systag_id_dict
    return selected_id[:num], systag_id_dict


def get_relation_plan2_step3(clinic_nos, num, is_baby=False):
    '''
    从clinic_nos召回热卖方案
    :param clinic_nos:
    :return:
    '''
    id_score = defaultdict(int)
    for clinic_no in clinic_nos:
        systag_id_list = get_db_data_local_handler().clinic_no_relation_systag_id(clinic_no)
        print 'systag_id_list', systag_id_list, clinic_no
        for systag_id in systag_id_list:
            if not is_baby and systag_id in baby_systag_ids:
                continue
            id_score[systag_id] += 1

    # 排序
    sorted_id_score = sorted(id_score.iteritems(), key=lambda x: x[1], reverse=True)
    # selected_plan = []
    # for id, score in sorted_id_score:
    #     plan = get_db_data_local_handler().get_systag_relation_plan(id)
    #     selected_plan.extend(plan)

    # return selected_plan
    selected_id = [item[0] for item in sorted_id_score]
    return selected_id[:num]


def is_recent(recent, ts):
    return int(ts) >= int(time.time() - recent) * 1000


def get_relation_plan3(uid, topn=4, recent=30 * 86400.0, test=False):
    '''
    step1 获取bs qa sd数据，保留时间戳并按时间排序，若最近时间小于1个月（s1=1) 且 能匹配上方案(s2=1) 直接返回，否则step2
    step2 获取view actions数据，保留时间戳并按照时间排序(若s2=1则跟step1的数据一起排序），若最近时间小于1个月（ss1=1)且 能匹配上方案（ss2=1)则直接返回，否则step3
    step3 若s2=1(step1的数据因为时间原因没有使用，就返回step1的结果）若s2=0,若ss2=1返回step2的结果，否则才使用clinic_no来匹配
    :param uid:
    :param num:
    :param test:
    :return:
    '''
    bad_return = {
        'ids': [],
        'status': 0
    }
    # step1
    is_baby = False
    t1 = time.time()
    text_dict1, ts_dict1, clinic_nos = get_user_query3(uid, num=5)
    # query, clinic_nos = get_user_query2(uid, num=5)  # 取5个搜索

    for key in text_dict1:
        print "text_dict1", key, text_dict1[key]
    for key in ts_dict1:
        print "ts_dict1", key, ts_dict1[key]

    t2 = time.time()
    print "get_user_query3 time", t2 - t1
    text_dict2, ts_dict2 = get_user_qa_content_smart2(uid, num=2)  # 取2个qa
    t3 = time.time()
    print "get_user_qa_content_smart2 time", t3 - t2
    # 合并搜索text_dict和qa的text_dict
    text_dict1.update(text_dict2)
    # 合并并排序搜索ts_dict和qa的ts_dict
    ts_dict1.update(ts_dict2)
    is_baby = is_baby_text(' '.join(text_dict1.values()))
    sorted_ts_dict1 = sorted(ts_dict1.iteritems(), key=lambda x: x[1], reverse=True)
    if sorted_ts_dict1:
        last_ts = sorted_ts_dict1[0][1]
        if is_recent(recent, last_ts):
            # 如果最近时间合适，则先取最近的texts
            recent_keys = [item[0] for item in sorted_ts_dict1 if is_recent(recent, item[1])]
            recent_texts = [text_dict1[key] for key in recent_keys]
            concat_text = ' '.join(recent_texts)

            selected_ids, systag_id_dict = get_relation_plan2_step1(concat_text, num=topn, is_baby=is_baby)
            if selected_ids:
                res = {
                    "ids": selected_ids,
                    "status": 1,
                }
                if test:
                    res['systag_id_dict'] = systag_id_dict
                    res['last_ts'] = last_ts
                return res
            else:
                # 没匹配上
                step1_bad = 1
        else:
            # 时间不最近
            step1_bad = 2

    else:
        # 没内容
        step1_bad = 1

    # 由于时间或者内容的原因step1没有成功返回，开始获取view actions data
    # step2
    t1 = time.time()
    text_dict3, ts_dict3 = user_view_action_texts(uid, 5)
    t2 = time.time()
    print "user_view_action_texts time", t2 - t1
    sorted_ts_dict3 = sorted(ts_dict3.iteritems(), key=lambda x: x[1], reverse=True)
    if sorted_ts_dict3:
        last_ts = sorted_ts_dict3[0][1]
        if is_recent(recent, last_ts):
            recent_keys = [item[0] for item in sorted_ts_dict3 if is_recent(recent, item[1])]
            recent_texts = [text_dict3[key] for key in recent_keys]
            selected_ids, systag_id_dict = get_relation_plan2_step1(' '.join(recent_texts), num=topn, is_baby=is_baby)
            if selected_ids:
                res = {
                    "ids": selected_ids,
                    "status": 2,
                }
                if test:
                    res['systag_id_dict'] = systag_id_dict
                    res['last_ts'] = last_ts
                return res
            else:
                step2_bad = 1
        else:
            step2_bad = 2
    else:
        step2_bad = 1

    # step1 and step2 都没有合适的最近的匹配上的
    # step3 若都是因为内容问题失败的，则尝试用clinic_nos
    if step1_bad == 1 and step2_bad == 1:
        select_ids = get_relation_plan2_step3(clinic_nos, num=topn, is_baby=is_baby)
        if select_ids:
            res = {
                'ids': select_ids,
                'status': 3
            }
            if test:
                res['clinic_no'] = clinic_nos
                last_ts = sorted_ts_dict1[0][1] if sorted_ts_dict1 else 0
                res['last_ts'] = last_ts
            return res


        else:
            # 毛也没有

            return bad_return
    else:
        # step1 step2至少有一个是因为时间问题不行的
        # 将所有信息按时间排序，然后取前5个
        ts_dict_total = ts_dict1
        ts_dict_total.update(ts_dict3)
        text_dict_total = text_dict1
        text_dict_total.update(text_dict3)
        sorted_ts_dict_total = sorted(ts_dict_total.iteritems(), key=lambda x: x[1], reverse=True)
        selected_keys = [item[0] for item in sorted_ts_dict_total[:5]]
        selected_texts = [text_dict_total[key] for key in selected_keys]
        selected_ids, systag_id_dict = get_relation_plan2_step1(' '.join(selected_texts), num=topn)
        if selected_ids:
            res = {
                'ids': selected_ids,
                'status': 4,
            }
            if test:
                res['systag_id_dict'] = systag_id_dict
                last_ts = sorted_ts_dict_total[0][1] if sorted_ts_dict_total else 0
                res['last_ts'] = last_ts
            return res
    return bad_return


def find_systag_keywords(text, max_len=7):
    '''
    找出keyword及其出现的词数和对应的systag_id的list
    '''
    cnt_dict = defaultdict(int)
    systag_id_dict = defaultdict(list)
    text = ensure_unicode(text)
    l = len(text)
    for begin_index in range(len(text)):
        cnt = 0
        for end_index in range(begin_index + 1, l + 1):
            if cnt >= max_len:
                continue
            w = text[begin_index:end_index]
            relation_systag_id = get_db_data_local_handler().get_keyword_relation_systag_id(w)
            if relation_systag_id:
                cnt_dict[w] += 1
                systag_id_dict[w] = relation_systag_id
            cnt += 1
    return systag_id_dict, cnt_dict


def find_systag_keywords_extend(text, max_len=7):
    cnt_dict = defaultdict(int)
    systag_id_dict = defaultdict(list)
    text = ensure_unicode(text)
    l = len(text)
    for begin_index in range(len(text)):
        cnt = 0
        for end_index in range(begin_index + 1, l + 1):
            if cnt >= max_len:
                continue
            w = text[begin_index:end_index]
            relation_systag_id, weight = get_db_data_local_handler().get_extend_keyword_relation_systag_id(w)
            if relation_systag_id:
                cnt_dict[w] += weight
                systag_id_dict[w] = relation_systag_id
            cnt += 1
    return systag_id_dict, cnt_dict


def get_user_qa_content_smart(uid, num=5):
    all_qa_text = []
    sql1 = 'select id from ask_problem where user_id=%s order by created_time limit %s;' % (uid, num)
    t1 = time.time()
    o1 = get_medicaldb_handler().do_one(sql1)
    t2 = time.time()
    print "get_user_qa_content_smart mysql time", t2 - t1
    if o1 is None or len(o1) == 0:
        return all_qa_text
    for item in o1:
        problem_id = item[0]
        print '-' * 10
        t1 = time.time()
        qa_texts = get_qa_texts_by_pid(problem_id)
        t2 = time.time()
        print "get_qa_texts_by_pid time", problem_id, t2 - t1
        all_qa_text.extend(qa_texts)
    return all_qa_text


def get_user_qa_content_smart2(uid, num=5):
    # sql1 = 'select id, created_time from ask_problem where user_id=%s order by created_time limit %s;' % (uid, num)
    # t1 = time.time()
    #
    # try:
    #     o1 = get_medicaldb_handler().do_one(sql1)
    # except:
    #     o1 = None
    #
    # t2 = time.time()
    # print "get_user_qa_content_smart2 mysql time", t2 - t1
    # if o1 is None or len(o1) == 0:
    #     return text_dict, ts_dict
    # pid_list = []
    #
    # for item in o1:
    #     problem_id = item[0]
    #     created_time = int(datetime_str2timestamp(item[1]) * 1000)
    #     pid_list.append(problem_id)
    #     #qa_texts = get_qa_texts_by_pid(problem_id)
    #     key = 'qa_' + str(problem_id)
    #     #text_dict[key] = ' '.join(qa_texts)
    #     ts_dict[key] = created_time
    t1 = time.time()
    ts_dict = dict(user_last_pids(uid, num=num, key_prefix="qa_"))
    t2 = time.time()
    print "user_last_pids time", t2 - t1
    pid_list = [item.split('_')[1] for item in ts_dict.keys()]

    t3 = time.time()
    print 'get_user_qa_content_smart2 mid time', t3 - t2
    text_dict = get_qa_texts_by_pids(pid_list)
    t4 = time.time()
    print "get_qa_texts_by_pids time", t4 - t3

    for key in text_dict:
        print "text_dict", key, text_dict[key]
    for key in ts_dict:
        print "text_dict", key, ts_dict[key]

    return text_dict, ts_dict


def get_user_qa_content2(uid, begin, end):
    # 从habse problem2表中 获取用户在一段时间内所有qa的全文
    begin_ds = timestamp2datetime(ensure_second_timestamp(begin))
    end_ds = timestamp2datetime(ensure_second_timestamp(end))

    all_qa_text = []

    sql1 = 'select id from ask_problem where user_id=%s and created_time>"%s" and created_time<"%s";' \
           % (
               uid, begin_ds, end_ds
           )

    o1 = get_medicaldb_handler().do_one(sql1)
    if o1 is None or len(o1) == 0:
        return all_qa_text

    for item in o1:
        problem_id = item[0]
        qa_texts = get_qa_texts_by_pid(problem_id)
        all_qa_text.extend(qa_texts)
    return all_qa_text


def get_user_qa_content(uid, begin, end):
    # 不要了，改用从hbase取数据
    # 获取用户在一段时间内所有qa的全文
    begin_ds = timestamp2datetime(ensure_second_timestamp(begin))
    end_ds = timestamp2datetime(ensure_second_timestamp(end))

    all_qa_text = []

    sql1 = 'select id from ask_problem where user_id=%s and created_time>"%s" and created_time<"%s";' \
           % (
               uid, begin_ds, end_ds
           )

    o1 = get_medicaldb_handler().do_one(sql1)
    if o1 is None or len(o1) == 0:
        return all_qa_text

    for item in o1:
        problem_id = item[0]
        sql = 'select content from ask_problemcontent where problem_id=%s;' % problem_id
        o = get_medicaldb_handler().do_one(sql)
        if o is None or len(o) == 0:
            continue

        content = o[0][0]
        content_dict = json.loads(content)[0]
        if content_dict['type'] != 'text':
            continue
        text = content_dict['text']
        all_qa_text.append(text)

    return all_qa_text


def user_last_view_actions(uid, num=10):
    '''
    用户最后的num个点击行为信息
    优先从cy_real_time_event中取，没有就再从solr md4 topics_profile and news_profile中取（数据不全，凑合用吧）
    :param uid:
    :param num:
    :return:
    '''
    ts_dict = {}
    # 从cy_real_time_event
    actions = get_user_recent_views(uid, num=num)
    # actions = [[ts,action_type,action_id],...]

    if actions:
        topic_ids = [['topic_' + str(item[2]), int(item[0])] for item in actions if 'topic' in item[1]]
        news_ids = [['news_' + str(item[2]), int(item[0])] for item in actions if 'news' in item[1]]
        ts_dict.update(dict(topic_ids))
        ts_dict.update(dict(news_ids))
        return ts_dict

    # 从solr
    news_row_key_list = get_cy_event_row_key_news(uid, num=num)
    topic_row_key_list = get_cy_event_row_key_topic(uid, num=num)
    news_row_key_list = [item['id'] for item in news_row_key_list]
    topic_row_key_list = [item['id'] for item in topic_row_key_list]
    topic_ids = get_news_id_from_cy_event2(topic_row_key_list, 'info:topic_id')
    news_ids = get_news_id_from_cy_event2(news_row_key_list, 'info:news_id')

    ts_dict.update(topic_ids)
    ts_dict.update(news_ids)
    return ts_dict


def user_view_action_texts(uid, num=10):
    ts_dict = user_last_view_actions(uid, num=num)
    text_dict = dict([[key, nat_get_title(key) + ' ' + nat_get_digest(key)] for key in ts_dict])
    return text_dict, ts_dict


def systagid_2_planid(systagids, num=4):
    # 将systagid_list 调整为 planid_list
    plan_id_list = []
    for x in systagids:
        plan_id_list.extend(get_db_data_local_handler().get_systagid_relation_planid(x))
    return plan_id_list[:num]


def test():
    import sys
    import time
    uid = sys.argv[1]
    t1 = time.time()
    res = get_relation_plan3(uid, test=True)
    t2 = time.time()
    print 'time', t2 - t1
    print json.dumps(res)


def test1():
    o = get_user_last_query(33360588)
    print o, type(o)


if __name__ == '__main__':
    test1()
