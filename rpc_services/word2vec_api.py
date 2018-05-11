# encoding=utf8

from __future__ import absolute_import
import numpy as np
import time
import json
import math
from chunyu.utils.general.encoding_utils import ensure_unicode
from rpc_services import rpc_clients
from recommend.manager.redis_utils import Word2VecCache, NOT_IN_REDIS_SIGN, NOT_IN_WORD2VEC_SIGN, \
    NOT_IN_WORD2VEC_SIGN_API, NOT_IN_WORD2VEC_SIMILAR_SIGN, NOT_IN_WORD2VEC_SIMILAR_SIGN_API

BAD_VEC_SIGN = [NOT_IN_REDIS_SIGN, NOT_IN_WORD2VEC_SIGN, NOT_IN_WORD2VEC_SIGN_API]


def get_vec_list(word_list):
    # 向量没有归一化
    # 返回的是一个字典
    input_arg = json.dumps(list(word_list))
    try:
        vec_dict = json.loads(rpc_clients.get_word2vec_service_client().get_w2v_vector_of_words(input_arg))
        return vec_dict
    except Exception, e:
        print 'get_vec_list exception', e
        return {}


def norm_list(alist):
    if alist is None:
        return []
    if not alist:
        return alist
    norm = math.sqrt(sum([x * x for x in alist]))
    return [x / norm for x in alist]


def get_vec_list_redis(word_list):
    # get_vec_list的redis版本，但向量归一化了,是list，不是ndarray
    # step 1 尝试从redis获取vec_list,不在Redis的NOT_IN_REDIS_SIGN表示
    word_list = [ensure_unicode(x) for x in word_list]
    vec_dict = {}
    not_in_redis_indices = []
    redis_res = Word2VecCache.get_vec_list(word_list)  #

    # 更换数据期间，所有词假设都不在redis中，而从api中重新获取，故redis_res = [NOT_IN_REDIS_SIGN]*len(word_list)
    # redis_res = [NOT_IN_REDIS_SIGN]*len(word_list)
    for i, vec in enumerate(redis_res):
        # vec 是归一化了的
        if vec == NOT_IN_REDIS_SIGN:
            not_in_redis_indices.append(i)
        else:
            if vec not in BAD_VEC_SIGN:  # 只加入有向量的词
                vec_dict[word_list[i]] = vec
    # step 2 redis里没有的，从接口里取，并把它们放进redis
    not_in_redis_word_list = [word_list[i] for i in not_in_redis_indices]

    # print 'not_in_redis_word_list', '|||'.join(not_in_redis_word_list)
    # for x in vec_dict:
    #     print 'already in redis good word', x, len(vec_dict[x])

    vec_dict_from_api = get_vec_list(not_in_redis_word_list)

    # for x in vec_dict_from_api:
    #     print 'vec_dict_from_api', x, len(vec_dict_from_api[x])

    for word in vec_dict_from_api:
        vec = vec_dict_from_api[word]
        vec = norm_list(vec)  # 归一化

        if vec != NOT_IN_WORD2VEC_SIGN_API:  # 只加入有向量的词,word2vec接口返回的
            vec_dict[word] = vec
        Word2VecCache.set_vec(word, vec)  # 存入redis

    # vec_dict.update(vec_dict_from_api)

    # for x in vec_dict:
    #     vec = vec_dict[x]
    #     print 'final word', x, type(x),len(vec), type(vec), math.sqrt(sum([y * y for y in vec]))

    return vec_dict


def get_vec_list_norm_ndarray(word_list):
    # 向量转化为ndarray且归一化，返回list
    # 不用了
    input_arg = json.dumps(list(word_list))
    try:
        vec_dict = json.loads(rpc_clients.get_word2vec_service_client().get_w2v_vector_of_words(input_arg))
        vec_list = []
        for vec in vec_dict.values():
            if vec:
                vec = np.array(vec)
                vec /= np.linalg.norm(vec)
                if vec not in vec_list:
                    vec_list.append(vec)

        return vec_list
    except Exception, e:
        print 'get_vec_list_norm_ndarray exception', e
        return []


def get_vec_list_norm_ndarray_redis(word_list):
    # 向量转化为ndarray且归一化，返回list,跟get_vec_list_norm_ndarray相比，这个顺序可能会打乱
    # step 1 尝试从redis获取vec_list,不在Redis的NOT_IN_REDIS_SIGN表示
    word_list = list(word_list)
    redis_res = Word2VecCache.get_vec_list(word_list)

    # 更换数据期间，所有词假设都不在redis中，而从api中重新获取，故redis_res = [NOT_IN_REDIS_SIGN]*len(word_list)
    # redis_res = [NOT_IN_REDIS_SIGN]*len(word_list)

    not_in_redis_indices = [i for i in range(len(redis_res)) if redis_res[i] == NOT_IN_REDIS_SIGN]
    vec_list = [vec for vec in redis_res if vec not in BAD_VEC_SIGN]
    # step 2 redis里没有的，从接口里取，并把它们放进redis
    not_in_redis_word_list = [word_list[i] for i in not_in_redis_indices]
    vec_dict_from_api = get_vec_list(not_in_redis_word_list)

    for word in vec_dict_from_api:
        vec = vec_dict_from_api[word]
        vec = norm_list(vec)  # 归一化
        # 若是非向量词，vec=[]
        Word2VecCache.set_vec(word, vec)  # 存入redis
        if vec != NOT_IN_WORD2VEC_SIMILAR_SIGN_API:
            vec_list.append(vec)

    return [np.array(vec) for vec in vec_list]  # 转换为ndarray


def get_vec_dict_norm_ndarray(word_list):
    # 向量转化为ndarray且归一化,返回字典
    # 不用了
    input_arg = json.dumps(list(word_list))
    try:
        vec_dict = json.loads(rpc_clients.get_word2vec_service_client().get_w2v_vector_of_words(input_arg))

        for w in vec_dict:
            vec = vec_dict[w]
            if not vec:
                vec_dict[w] = None
            else:
                vec = np.array(vec)
                vec /= np.linalg.norm(vec)
                vec_dict[w] = vec
        return vec_dict
    except Exception, e:
        print 'get_vec_dict_norm_ndarray exception', e
        return {}


def get_vec_dict_norm_ndarray_redis(word_list):
    # 向量转化为ndarray且归一化,返回字典
    vec_dict = get_vec_list_redis(word_list)
    # 转换成ndarray
    for word in vec_dict:
        nd_vec = np.array(vec_dict[word])
        vec_dict[word] = nd_vec
    return vec_dict


def get_vec(word):
    bad_return = None
    try:
        vec = np.array(json.loads(rpc_clients.get_word2vec_service_client().get_w2v_vector_of_word(word)))
        if vec is None:
            return bad_return
        return vec / np.linalg.norm(vec)
    except Exception, e:
        print "get_vec exception", e
        return bad_return


def get_similar(word, num=10):
    bad_return = []
    try:
        return json.loads(rpc_clients.get_word2vec_service_client().get_w2v_most_similar(word, num))
    except Exception, e:
        print "get_similar exception", e
        return bad_return


def get_similar_redis(word, num=30):
    # get_similar 的 redis 版
    # step 1 尝试从redis里边取
    print 'get_similar_redis',word
    similar_word_list = Word2VecCache.get_similar(word, num)
    # if isinstance(similar_word_list,list):
    #     for w,s in similar_word_list:
    #         print '-'*15
    #         print 'similar word from redis',w,s
    # else:
    #     print 'similar word from redis',similar_word_list

    if similar_word_list == NOT_IN_REDIS_SIGN:
        # 从api取
        similar_word_list = get_similar(word, num)
        # for w,s in similar_word_list:
        #     print '+'*15
        #     print 'similar word from api',w,s
        # 存入redis
        Word2VecCache.set_similar(word, similar_word_list)

    return similar_word_list


def get_similar_list(word_list, num=10):
    # 速度很慢，不用了
    bad_return = []
    input_arg = json.dumps(list(word_list))
    try:
        return json.loads(rpc_clients.get_word2vec_service_client().get_w2v_most_similar_of_words(input_arg, num))
    except Exception, e:
        print "get_similar_list exception", e
        return bad_return


def get_vecs2(words):
    # 不考虑权重
    words = [ensure_unicode(x) for x in words]
    return [get_vec(word) for word in words if get_vec(word) is not None]


def get_vecs_weighted(words, weights):
    # 不再用
    vecs = []
    new_weights = []
    words = [ensure_unicode(x) for x in words]
    for i in range(len(words)):
        vec = get_vec(words[i])
        if vec is None:
            continue
        vecs.append(vec)
        new_weights.append(weights[i])
    return vecs, new_weights


def get_vecs_weighted2(words):
    # 给出那些保留的indices
    # 不再用
    keep_indices = []
    vecs = []
    words = [ensure_unicode(x) for x in words]
    for i in range(len(words)):
        vec = get_vec(words[i])
        if vec is None:
            continue
        vecs.append(vec)
        keep_indices.append(i)

    return vecs, keep_indices


def get_vecs_weighted3(words):
    # 与get_vecs_weighted2功能一样，但使用word2vec的get_vec_list_redis接口
    vec_dict = get_vec_list_redis(list(words))
    # vec_dict 的key 确保都是unicode编码的
    # 已经归一化，并去掉word2vec里边没有向量的,但是没有转换为ndarray

    keep_indices = []
    vecs = []
    for i, word in enumerate(words):
        word = ensure_unicode(word)
        if word in vec_dict:
            keep_indices.append(i)
            vecs.append(np.array(vec_dict[word]))
    return vecs, keep_indices


def test_all():
    # 需要测试一下api版本和redis版本的输出是否一致
    from random import choice
    word_list = ['redis', '', '肺气肿',
                 'b超', '唐筛', '肺弥散功能障碍', ' ',
                 '糖尿病','感冒','清鼻涕','黄鼻涕','鼻炎','鼻涕',
                 '甘油三酯','肾错构瘤','发烧','退烧药','温度高',
                 '智障','haha','蜂蜜','哦也']
    t1 = time.time()
    vec_list = get_vec_list_norm_ndarray_redis(word_list)
    t2 = time.time()
    print 'time',t2 - t1
    for x in vec_list:
        print type(x),np.linalg.norm(x)




def test():
    w1 = "脚"
    w2 = "手"
    v1 = get_vec(w1)
    v2 = get_vec(w2)
    print v1
    print v2

    print np.dot(v1, v2)


def test1():
    import sys
    w = sys.argv[1]
    o = get_similar_redis(w, 10)
    for w, s in o:
        print w, s


def test2():
    word_list = ['', '手', '脚', '眼睛', '纸抽', None]
    word_list = '霉菌性阴道炎-阳性-乙肝表面抗体阳性-甘油三酯-肾错构瘤-宫颈肥大-胆固醇高-发烧-退烧药-晚饭-温度高-呕吐-全身-发抖-流感-急性上呼吸道感染-脊柱侧弯-身高-阴茎延长术-阴唇-整形-整形术-黑眼圈-牙齿-牙齿黄-扁桃体炎-治国-尿-白蛋白-肌酐-肾-活检-慢性肾炎-隐匿性肾炎-肾损害-肾炎-高脂血症-高胰岛素血症-月经不规律-月经-射-女友-肚皮-怀孕-精液-手指-阴道口-摸'.split(
        '-')
    print len(word_list)
    t1 = time.time()
    vecs = get_vec_dict_norm_ndarray(word_list[:20])
    t2 = time.time()
    print t2 - t1
    print len(vecs)
    for x in vecs:
        l = len(vecs[x]) if vecs[x] is not None else 0
        print x, type(x), l


def test3():
    # test get_similar_list
    word_list = '霉菌性阴道炎-阳性-乙肝表面抗体阳性-甘油三酯-肾错构瘤-宫颈肥大-胆固醇高-发烧-退烧药-晚饭-温度高-呕吐-全身-发抖-流感-急性上呼吸道感染-脊柱侧弯-身高-阴茎延长术-阴唇-整形-整形术-黑眼圈-牙齿-牙齿黄-扁桃体炎-治国-尿-白蛋白-肌酐-肾-活检-慢性肾炎-隐匿性肾炎-肾损害-肾炎-高脂血症-高胰岛素血症-月经不规律-月经-射-女友-肚皮-怀孕-精液-手指-阴道口-摸'.split(
        '-')
    print len(word_list)
    for x in word_list:
        o = get_similar(x, 50)
        print o

if __name__ == '__main__':
    test1()
