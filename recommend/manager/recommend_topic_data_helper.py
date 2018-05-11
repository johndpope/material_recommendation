# encoding=utf8

import sys
import time
import json

from recommend.manager.data_helper import get_sex, special_population_big, \
    weights_normalization
from rpc_services.medical_service_utils import get_medical_entities_info3,get_medical_entities_info3_cyseg
from rpc_services.word2vec_api import get_vecs_weighted3
from general_utils.hbase_utils import yesterday_user_info, get_sp_duration_valid_user_data, \
    get_sp_duration_valid_user_id
from general_utils.textrank_utils import cal_tags_textrank
from general_utils.time_utils import ensure_second_timestamp, get_yesterday_timestamp
from general_utils.solr_utils import get_caled_user_topn_topics_yesterday


def get_yesterday_user_info(uid, timestamp=None):
    return yesterday_user_info(uid, timestamp)


def get_all_yesterday_user_info(now=None, test=False):
    if not now:
        now = time.time()
    else:
        now = ensure_second_timestamp(now)

    begin, end = get_yesterday_timestamp(now)
    if test:
        end = begin + 30 * 60  # 测试模式只取三十分钟数据

    user_info0 = get_sp_duration_valid_user_data(begin, end)

    return user_info0


def get_all_yesterday_user_id(now=None, test=False):
    if not now:
        now = time.time()
    else:
        now = ensure_second_timestamp(now)

    begin, end = get_yesterday_timestamp(now)
    if test:
        end = begin + 30 * 60  # 测试模式只取三十分钟数据

    all_valid_uids = get_sp_duration_valid_user_id(begin, end)
    return all_valid_uids


def parse_user_info(user_info):
    # 和data_helper.parse_user_info的区别是这个不仅仅取最后一个信息，而是取user_info中所有的信息
    '''
    texts = [item[0] for item in user_info['big_search']]
        text = ' '.join(texts).strip()

        # tags, weights, cates, _ = get_medical_entities_info([text], [1.0], 1.0)
        # 改为不仅仅使用医学实体词
        tags, weights, cates, counts, entities, entity_counts = get_medical_entities_info2(
            text_list=[text],
            weight_list=[1.0],
            neg=False
        )

        # vecs, weights = get_vecs_weighted(tags, weights)
        vecs, keep_indices = get_vecs_weighted2(tags)
        weights = [weights[i] for i in keep_indices]
        # cates = [cates[i] for i in keep_indices]
        tags = [tags[i] for i in keep_indices]  # 去掉没有向量的tag
        entities = [x for x in entities if x in tags]  # 去掉没有向量的实体词
        # 即使vecs is None，还是可以用big_search搜出来点啥，就是不能rank了
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
    '''
    age = None
    qa_sex = None
    qa_texts = []  #######需不需要对时间进行衰减
    bs_texts = [item[0] for item in user_info['big_search']] if 'big_search' in user_info else []
    if 'free_problem_create' in user_info:
        for text, s_sex, s_age, _ in user_info['free_problem_create']:
            age = s_age
            qa_sex = s_sex
            qa_texts.append(text)
    texts = bs_texts + qa_texts
    all_concat_text = ' '.join(texts)
    if qa_sex:
        sex = get_sex(qa_sex)
    else:
        sex = get_sex(all_concat_text)

    special_population, _ = special_population_big(all_concat_text)
    words_weights_cates_vecs = get_words_weights_cates_vecs(text_list=qa_texts + bs_texts,
                                                            weight_list=[1.0] * len(texts)
                                                            )

    res = {
        'texts': texts,
        'trigger': 'big_search',
        'sex': sex,
        'age': age,
        'special_population': special_population,

    }

    res.update(words_weights_cates_vecs)
    return res


def get_words_weights_cates_vecs(text_list, weight_list):
    seg_res = get_medical_entities_info3_cyseg(text_list=text_list,
                                         weight_list=weight_list)
    tags = seg_res['words']
    all_tags = seg_res['all_words']
    tags1 = seg_res['words1']
    weights = seg_res['weights']
    cates = seg_res['cates']
    entities = seg_res['entities']

    text_rank = cal_tags_textrank(node_tags=tags1,
                                  edge_tags=all_tags)

    for i in range(len(tags)):
        print '====', tags[i], weights[i]

    vecs, keep_indices = get_vecs_weighted3(tags)
    weights = weights_normalization([weights[i] * text_rank[tags[i]] for i in keep_indices])
    tags = [tags[i] for i in keep_indices]  # 去掉没有向量的tag
    entities = [x for x in entities if x in tags]  # 去掉没有向量的实体词

    return {
        'tags': tags,
        'vecs': vecs,
        'weights': weights,
        'entities': entities,
        'cates': cates
    }


def get_caled_user_topn_topics_yesterday0(uid):
    return get_caled_user_topn_topics_yesterday(uid)



def test():
    text_list = [
        "我2014年的时候有过高危性行为找过小姐，期间也吸食过冰毒麻果，不过没有打针这些的，就是吸食，然后我2015年，年头我戒了冰毒，身体就发胖，胖到146斤右左，可是到2016年头，我左上腹痛，然后跑到右上腹痛，拉着整个后背都痛，我这两年做的检查不少，现在喉咙处有时也会痛，舌头干涩，体重从16年的146多斤减到现在110斤，整天整个人头晕晕的，我怕我是艾滋病，我不敢去查，医生你说我这是艾滋吗",
        "鼻子两侧会长白头，一粒粒的那种，嵌在皮肤中，摸起来很粗糙，还有一些黑头和螨虫，请问应该怎么治疗",
        "内科",
        "六味地黄丸", "脊髓空洞", "六味地黄丸脊髓空洞"]

    res = get_words_weights_cates_vecs(text_list, [1.0] * len(text_list))

    tags = res['tags']
    vecs = res['vecs']
    weights = res['weights']
    e = res['entities']
    cates = res['cates']

    print 'tags', '-'.join(tags)
    print len(tags), len(weights), len(vecs)
    for i in range(len(tags)):
        print tags[i], weights[i]

    print '-'.join(e)
    for x in cates:
        print x, cates[x]


def test1():
    user_info0 = get_all_yesterday_user_info(test=True)
    for uid in user_info0:
        print '*' * 10, uid, '*' * 10
        print json.dumps(user_info0[uid])


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'test':
        test1()
