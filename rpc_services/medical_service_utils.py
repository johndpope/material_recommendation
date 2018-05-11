# encoding=utf8


import os
import sys
import time
from collections import defaultdict

from global_config import get_root_path
from rpc_services.medical_service_api import tokenizer as tokenizer_use
from rpc_services.medical_service_api import medical_ner
from general_utils.text_utils import get_stop_word_handler, is_number_all
from general_utils.db_utils import get_db_data_local_handler
from cy_seg.jieba_seg import tokenizer as tokenizer_cyseg

TEST_TEXT_FILE = os.path.join(get_root_path(), 'general_utils', 'general_data_dir', 'test_text.txt')

ALL_CATE = [
    "SYMPTOM_DESC",
    "DRUG_DESC",
    "DISEASE_DESC",
    "OPERATION_DESC",
    "CHECKUP_DESC",
    "CLINIC_DESC",
    "BODYPART_DESC"
]

CATE_WEIGHTS = {
    "SYMPTOM_DESC": 1.0,
    "DRUG_DESC": 1.0,
    "DISEASE_DESC": 2.0,
    "OPERATION_DESC": 1.0,
    "CHECKUP_DESC": 1.0,
    "CLINIC_DESC": 1.0,
    "BODYPART_DESC": 1.0,
}

CATE_NAME_MAP = {
    ''
}
BAD_WORDS = {
    u"疼": 0.3,
    u"疼痛": 0.5,
    u"痛": 0.3,
    u"出血": 0.3,
    u"血": 0.3,
    u"怀孕": 0.6,
    u"肿": 0.3,
    u'做手术':0.3,
    u'手术':0.3,

}


def get_entities(text, neg=True, keep_cate=ALL_CATE):
    # 非正式场合使用
    flags = ["ner"]
    if neg:
        flags.append("neg_ner")

    bad_return = set()
    try:
        tokenizer_output = tokenizer_use([text], flags)
    except Exception, e:
        print e
        return bad_return
    tokens = tokenizer_output['tokens'][0]

    words = set()
    for item in tokens:
        if u'cate' in item:
            if u"neg_ne" in item:
                continue
            if item['cate'] not in keep_cate:
                continue
            words.add(item[u'token'])

    return words


def get_entities_cyseg(text, keep_cate=ALL_CATE):
    # 返回全部的医学实体词
    # get_entities的cyseg版本
    tokens = tokenizer_cyseg(text, new_word=True)
    return set([token.strip() for token in tokens if get_db_data_local_handler().is_entity(token)])


def test_get_entities():
    print '*' * 10, "get_entities", '*' * 10

    with open(TEST_TEXT_FILE, 'r') as f:
        for l in f:
            text = l.strip('\n')
            print '=' * 8, text

            t1 = time.time()
            o1 = get_entities(text, False)
            t2 = time.time()
            o2 = get_entities_cyseg(text)
            t3 = time.time()
            print 'use rpc', t2 - t1
            for x in o1:
                print x
            print 'cyseg', t3 - t2
            for x in o2:
                print x


# def get_entities_list(text, neg=True, keep_cate=ALL_CATE):
#     # 非正式场合使用
#     # 不用了
#     flags = ["ner"]
#     if neg:
#         flags.append("neg_ner")
#
#     bad_return = set()
#     try:
#         tokenizer_output = tokenizer_use([text], flags)
#     except Exception, e:
#         print e
#         return bad_return
#     tokens = tokenizer_output['tokens'][0]
#
#     words = []
#     for item in tokens:
#         if u'cate' in item:
#             if u"neg_ne" in item:
#                 continue
#             if item['cate'] not in keep_cate:
#                 continue
#             words.append(item[u'token'])
#
#     return words


# def segtext(text, filter_stopword=True):
#     # 不用了
#     flags = ["ner"]
#     bad_return = []
#     tokenizer_output = tokenizer_use([text], flags)
#     tokens = tokenizer_output['tokens'][0]
#     words = []
#     all_words = []
#     for item in tokens:
#         word = item['token']
#         all_words.append(word)
#         if get_stop_word_handler().is_stop_word(word):
#             continue
#         words.append(word)
#     return words, all_words


def get_entities_sp(text, keep_cate=ALL_CATE):
    '''
    额外返回 1 相关药品(relation_drug)；2 keep_cate以外的词
    '''
    bad_return = set(), set(), set(), set()  # words,other_words,relation_drug_dict

    flags = {"relation_drug_num": 2, "freq": 0}
    medical_ner_output = medical_ner([text], flags)
    entities = medical_ner_output['entities']
    if not entities:
        return bad_return

    words = set()
    other_words = set()
    relation_drug = set()
    other_relation_drug = set()
    tokens = entities[0]
    for item in tokens:
        cate = item['cate']
        word = item['name']
        if cate in keep_cate:
            words.add(word)
            relation_drug.update(set(item.get('relation_drug', [])))
        else:
            other_words.add(word)
            other_relation_drug.update(set(item.get('relation_drug', [])))

    return words, other_words, relation_drug, other_relation_drug


def get_entities_sp_cyseg(text, keep_cate=ALL_CATE):
    '''
    recommend_tags用到这个了
    额外返回 1 相关药品(relation_drug)；2 keep_cate以外的词
    的cyseg版本
    return: words, other_words, relation_drug, other_relation_drug
    words: 属于keep_cate的实体词
    other_words: 不属于keep_cate的实体词
    relation_drug: words的相关药品
    other_relation_drug: other_words的相关药品
    '''

    tokens = tokenizer_cyseg(text, False)  ####newwords=False
    words = set()
    other_words = set()
    relation_drug = set()
    other_relation_drug = set()
    relation_drug_num = 2
    for word in tokens:
        word = word.strip()
        if not word:
            continue
        cate = get_db_data_local_handler().get_entity_cate(word)
        if not cate:
            continue
        if cate in keep_cate:
            words.add(word)
            relation_drug.update(set(get_db_data_local_handler().get_relation_drug(word, num=relation_drug_num)))
        else:
            other_words.add(word)
            other_relation_drug.update(set(get_db_data_local_handler().get_relation_drug(word, num=relation_drug_num)))

    # medical_ner_output = medical_ner([text], flags)
    # entities = medical_ner_output['entities']
    # if not entities:
    #     return bad_return
    #
    # words = set()
    # other_words = set()
    # relation_drug = set()
    # other_relation_drug = set()
    # tokens = entities[0]
    # for item in tokens:
    #     cate = item['cate']
    #     word = item['name']
    #     if cate in keep_cate:
    #         words.add(word)
    #         relation_drug.update(set(item.get('relation_drug', [])))
    #     else:
    #         other_words.add(word)
    #         other_relation_drug.update(set(item.get('relation_drug', [])))

    return words, other_words, relation_drug, other_relation_drug


def test_get_entities_sp():
    KEEP_CATE = [
        "SYMPTOM_DESC",
        "DRUG_DESC",
        "DISEASE_DESC",
        "OPERATION_DESC",
        "CHECKUP_DESC",
        "CLINIC_DESC",
    ]
    print '*' * 10, 'test_get_entities_sp', '*' * 10
    with open(TEST_TEXT_FILE, 'r') as f:
        for l in f:
            text = l.strip('\n')
            print '=' * 8, text
            t1 = time.time()
            words1, other_words1, relation_drug1, other_relation_drug1 = get_entities_sp(text, KEEP_CATE)
            t2 = time.time()
            words2, other_words2, relation_drug2, other_relation_drug2 = get_entities_sp_cyseg(text, KEEP_CATE)
            t3 = time.time()

            print 'rpc', t2 - t1
            print 'words', '-'.join(words1)
            print 'other_words1', '-'.join(other_words1)
            print 'relation_drug', '-'.join(relation_drug1)
            print 'other_relation_drug', '-'.join(other_relation_drug1)

            print 'cyseg', t3 - t2
            print 'words', '-'.join(words2)
            print 'other_words1', '-'.join(other_words2)
            print 'relation_drug', '-'.join(relation_drug2)
            print 'other_relation_drug', '-'.join(other_relation_drug2)


# def get_medical_entities_info(text_list, weight_list, general_weight_factor=1.0, keep_cate=ALL_CATE, neg=True):
#     # 不要了
#     flags = ["ner", "neg_ner"]
#     tokenizer_output = tokenizer_use(text_list, flags)
#     tokens = tokenizer_output['tokens']
#     words = []
#     weights = []
#     cates = []
#     counts = []  # 每个text处理后words的长度（累积量）
#     for i in range(len(tokens)):
#         sentence_weight = general_weight_factor * weight_list[i]
#         for token_info in tokens[i]:
#             word = token_info['token']
#             if "cate" in token_info and token_info['cate'] in keep_cate:
#
#                 if neg and "neg_ne" in token_info:
#                     continue
#                 if word in words:
#                     continue
#                 cate = token_info['cate']
#                 word_weight = sentence_weight * CATE_WEIGHTS[cate]
#                 words.append(word)
#                 weights.append(word_weight)
#                 cates.append(cate)
#         counts.append(len(words))
#     return words, weights, cates, counts


def get_medical_entities_info2(text_list, weight_list, general_weight_factor=1.0, not_entity_factor=0.3,
                               neg=False, weights_is_dict=False):
    flags = ["ner"]
    if neg:
        flags.append("neg_ner")
    tokenizer_output = tokenizer_use(text_list, flags)
    tokens = tokenizer_output['tokens']

    words = []
    if weights_is_dict:
        weights = {}
    else:
        weights = []
    cates = {}
    counts = []  # 每个text处理后words的长度（累积量）
    entity_counts = []
    entities = []

    for i in range(len(tokens)):
        sentence_weight = general_weight_factor * weight_list[i]
        for token_info in tokens[i]:
            word = token_info['token'].strip()

            if word in words or not word:
                continue
            if get_stop_word_handler().is_stop_word(word) and 'cate' not in token_info:
                continue  # 去掉停止词
            if is_hospital_word(word):  # 去掉医院词
                continue
            try:
                _ = int(word)  # 去掉纯数字
                continue
            except:
                pass

            if neg and "neg_ne" in token_info:
                continue
            if 'cate' in token_info:
                if get_stop_word_handler().is_e_stop_word(word):
                    continue
                is_entity = True
                word_weight = sentence_weight * CATE_WEIGHTS[token_info['cate']]
                # cate = token_info['cate']
            else:
                is_entity = False
                word_weight = sentence_weight * not_entity_factor
                # cate = ''
            words.append(word)
            if word in BAD_WORDS:
                word_weight = BAD_WORDS[word]

            if weights_is_dict:
                weights[word] = word_weight
            else:
                weights.append(word_weight)

            if is_entity:
                entities.append(word)
                cates[word] = token_info['cate']

        counts.append(len(words))
        entity_counts.append(len(entities))

    return words, weights, cates, counts, entities, entity_counts


def get_medical_entities_info2_cyseg(text_list, weight_list, general_weight_factor=1.0, not_entity_factor=0.3,
                                     weights_is_dict=False):
    # 分词
    tokens_list = [tokenizer_cyseg(text, True) for text in text_list]

    # 返回值
    words = []
    if weights_is_dict:
        weights = {}
    else:
        weights = []
    cates = {}
    counts = []  # 每个text处理后words的长度（累积量）
    entity_counts = []
    entities = []

    # 整
    for i in range(len(tokens_list)):
        sentence_weight = general_weight_factor * weight_list[i]
        for word in tokens_list[i]:
            # 过滤一些情况
            word = word.strip()

            if word in words or not word:
                continue
            cate = get_db_data_local_handler().get_entity_cate(word)
            if get_stop_word_handler().is_stop_word(word) and cate == '':
                continue  # 去掉停止词
            if is_hospital_word(word):  # 去掉医院词
                continue
            if is_number_all(word):  # 去掉纯数字
                continue

            is_entity = cate != ''

            if is_entity:
                if get_stop_word_handler().is_e_stop_word(word):
                    continue
                word_weight = sentence_weight * CATE_WEIGHTS[cate]

            else:
                word_weight = sentence_weight * not_entity_factor

            words.append(word)
            if word in BAD_WORDS:
                word_weight = BAD_WORDS[word]

            if weights_is_dict:
                weights[word] = word_weight
            else:
                weights.append(word_weight)

            if is_entity:
                entities.append(word)
                cates[word] = cate

        counts.append(len(words))
        entity_counts.append(len(entities))

    return words, weights, cates, counts, entities, entity_counts


def test_get_medical_entities_info2():
    print '*' * 10, 'test_get_medical_entities_info2', '*' * 10

    with open(TEST_TEXT_FILE, 'r') as f:
        for l in f:
            text = l.strip('\n')
            print '=' * 8, text
            t1 = time.time()
            words1, weights1, cates1, counts1, entities1, entity_counts1 = get_medical_entities_info2(
                text_list=[text], weight_list=[1.0], general_weight_factor=1.0,
                neg=False
            )
            t2 = time.time()
            words2, weights2, cates2, counts2, entities2, entity_counts2 = get_medical_entities_info2_cyseg(
                text_list=[text], weight_list=[1.0], general_weight_factor=1.0
            )
            t3 = time.time()

            print 'rpc', t2 - t1
            print 'words', '-'.join(words1)
            print 'weights', weights1
            print 'cates', cates1
            print 'counts', counts1
            print 'entities', '-'.join(entities1)
            print 'entity_counts', entity_counts1

            print 'cyseg', t3 - t2
            print 'words', '-'.join(words2)
            print 'weights', weights2
            print 'cates', cates2
            print 'counts', counts2
            print 'entities', '-'.join(entities2)
            print 'entity_counts', entity_counts2


def get_medical_entities_info3(text_list, weight_list, general_weight_factor=1.0, not_entity_factor=0.3,
                               neg=False):
    # 返回的words可重复
    flags = ["ner"]
    if neg:
        flags.append("neg_ner")
    tokenizer_output = tokenizer_use(text_list, flags)
    tokens = tokenizer_output['tokens']

    words = []  # 顺序的实体词和非停止词,不重复
    words1 = []  # 顺序的实体词和非停止词,可重复
    all_words = []  # 顺序的所有词
    weights = []
    cates = {}
    counts = []  # 每个text处理后words的长度（累积量）
    entity_counts = []
    entities = []

    for i in range(len(tokens)):
        sentence_weight = general_weight_factor * weight_list[i]
        t_all_words = []
        t_words1 = []
        for token_info in tokens[i]:
            word = token_info['token'].strip()
            is_stop_word = get_stop_word_handler().is_stop_word(word) and 'cate' not in token_info
            t_all_words.append(word)
            if is_stop_word:
                continue

            t_words1.append(word)

            if word in words or not word:
                continue

            if is_hospital_word(word):  # 去掉医院词
                continue
            try:
                _ = int(word)  # 去掉纯数字
                continue
            except:
                pass

            if neg and "neg_ne" in token_info:
                continue
            if 'cate' in token_info:
                if get_stop_word_handler().is_e_stop_word(word):
                    continue
                is_entity = True
                word_weight = sentence_weight * CATE_WEIGHTS[token_info['cate']]
            else:
                is_entity = False
                word_weight = sentence_weight * not_entity_factor
            words.append(word)
            if word in BAD_WORDS:
                word_weight = BAD_WORDS[word]
            weights.append(word_weight)
            if is_entity:
                entities.append(word)
                cates[word] = token_info['cate']

        counts.append(len(words))
        entity_counts.append(len(entities))
        all_words.append(t_all_words)
        words1.append(t_words1)

    return {
        "words": words,
        "weights": weights,
        "cates": cates,
        "counts": counts,
        "entities": entities,
        "entity_counts": entity_counts,
        "all_words": all_words,  # 二维的
        "words1": words1  # 二维的
    }


def get_medical_entities_info3_cyseg(text_list, weight_list, general_weight_factor=1.0, not_entity_factor=0.3):
    # 返回的words可重复,get_medical_entities_info3的cyseg版本

    # 分词
    tokens_list = [tokenizer_cyseg(text, True) for text in text_list]

    # 返回值
    words = []  # 顺序的实体词和非停止词,不重复
    words1 = []  # 顺序的实体词和非停止词,可重复
    all_words = []  # 顺序的所有词
    weights = []
    cates = {}
    counts = []  # 每个text处理后words的长度（累积量）
    entity_counts = []
    entities = []

    for i in range(len(tokens_list)):
        sentence_weight = general_weight_factor * weight_list[i]
        t_all_words = []  # 包括停止词的，作为textrank的edge
        t_words1 = []  # 不包括停止词，作为textrank的节点
        for word in tokens_list[i]:
            word = word.strip()
            cate = get_db_data_local_handler().get_entity_cate(word)
            is_stop_word = get_stop_word_handler().is_stop_word(word) and cate == ''
            t_all_words.append(word)
            if is_stop_word:
                continue

            t_words1.append(word)

            if word in words or not word:
                continue

            if is_hospital_word(word):  # 去掉医院词
                continue
            if is_number_all(word):  # 去掉纯数字
                continue

            if cate:
                if get_stop_word_handler().is_e_stop_word(word):
                    continue
                is_entity = True
                word_weight = sentence_weight * CATE_WEIGHTS[cate]
            else:
                is_entity = False
                word_weight = sentence_weight * not_entity_factor
            words.append(word)
            if word in BAD_WORDS:
                word_weight = BAD_WORDS[word]
            weights.append(word_weight)
            if is_entity:
                entities.append(word)
                cates[word] = cate

        counts.append(len(words))
        entity_counts.append(len(entities))
        all_words.append(t_all_words)
        words1.append(t_words1)

    return {
        "words": words,
        "weights": weights,
        "cates": cates,
        "counts": counts,
        "entities": entities,
        "entity_counts": entity_counts,
        "all_words": all_words,  # 二维的
        "words1": words1  # 二维的
    }


def test_get_medical_entities_info3():
    '''
    "words": words,
        "weights": weights,
        "cates": cates,
        "counts": counts,
        "entities": entities,
        "entity_counts": entity_counts,
        "all_words": all_words,  # 二维的
        "words1": words1  # 二维的
    :return:
    '''
    print '*' * 10, 'test_get_medical_entities_info3', '*' * 10
    with open(TEST_TEXT_FILE, 'r') as f:
        for l in f:
            text = l.strip('\n')
            print '=' * 8, text
            t1 = time.time()
            res_dict1 = get_medical_entities_info3(
                text_list=[text],
                weight_list=[1.0],
                neg=False
            )
            t2 = time.time()
            res_dict2 = get_medical_entities_info3_cyseg(
                text_list=[text],
                weight_list=[1.0]
            )
            t3 = time.time()
            print 'rpc', t2 - t1
            print 'words', '-'.join(res_dict1['words'])
            print 'weights', res_dict1['weights']
            print 'cates', res_dict1['cates']
            print 'counts', res_dict1['counts']
            print 'entities', '-'.join(res_dict1['entities'])
            print 'entity_counts', res_dict1['entity_counts']

            print 'rpc', t3 - t2
            print 'words', '-'.join(res_dict2['words'])
            print 'weights', res_dict2['weights']
            print 'cates', res_dict2['cates']
            print 'counts', res_dict2['counts']
            print 'entities', '-'.join(res_dict2['entities'])
            print 'entity_counts', res_dict2['entity_counts']


def get_weighted_word(text, base_weight, not_entity_factor=0.3):
    '''
    将text分词，保留医学词和非停止词和它们的权重，失去顺序并将出现次数加入权重，并都乘上基础权重（base_weight)
    :param text: a string
    :param base_weight: a float
    :return: res is dict: key is word, value is its weight
    '''
    word_weight_dict = defaultdict(float)  # uword:weight
    cate_dict = {}  # uword:cate

    word_list = tokenizer_cyseg(text, True)

    for word in word_list:
        word = word.strip()
        cate = get_db_data_local_handler().get_entity_cate(word)
        is_stop_word = get_stop_word_handler().is_stop_word(word) and cate == ''

        if is_stop_word or not word:  # 过滤停止词和空字符串
            continue
        if is_hospital_word(word):  # 去掉医院词
            continue
        if is_number_all(word):  # 去掉纯数字
            continue

        word_weight = base_weight

        if word in BAD_WORDS:
            word_weight *= BAD_WORDS[word]

        if cate:
            cate_dict[word] = cate
            word_weight *= CATE_WEIGHTS[cate]
        else:
            word_weight *= not_entity_factor
        word_weight_dict[word] += word_weight

    return word_weight_dict, cate_dict


def test_get_weighted_word():
    get_weighted_word('头孢怎么吃才能治疗高血压', 3)


def add_word_weight_dict(dict0, dict1):
    for k in dict1:
        dict0[k] += dict1[k]


def get_weighted_word_list(text_tf_list):
    # tf is short for 'time factor'
    # text_tf_list = [[text1,tf1],..]

    big_cate_dict = {}
    big_word_weight_dict = defaultdict(float)
    for text, tf in text_tf_list:
        word_weight_dict, cate_dict = get_weighted_word(text, tf)
        # 合并word_weight_dict
        add_word_weight_dict(big_word_weight_dict, word_weight_dict)
        big_cate_dict.update(cate_dict)

    return big_word_weight_dict, big_cate_dict


def test_get_weighted_word_list():
    text_tf_list = [['感冒了怎么办词典', 10.0], ['脚气想吃莎普爱思和腰痛宁胶囊语音', 0.1]]
    big_word_weight_dict, big_cate_dict = get_weighted_word_list(text_tf_list)
    for x in big_word_weight_dict:
        print 'big_word_weight_dict', x, big_word_weight_dict[x]
    for x in big_cate_dict:
        print 'big_cate_dict', x, big_cate_dict[x]


def is_hospital_word(uword):
    if u"医大" in uword:
        return True
    if uword.endswith(u"院"):
        return True
    return False


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'test':
        choice = sys.argv[2]
        if choice == '1':
            test_get_entities()
        elif choice == '2':
            test_get_entities_sp()
        elif choice == '3':
            test_get_medical_entities_info2()
        elif choice == '4':
            test_get_medical_entities_info3()
        elif choice == '5':
            test_get_weighted_word()
        elif choice == '6':
            test_get_weighted_word_list()
