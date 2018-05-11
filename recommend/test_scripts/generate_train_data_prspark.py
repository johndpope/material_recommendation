# encoding=utf8

import json
from general_utils.spark_env import *


# def load_lines(fin):
#     items = []
#     with open(fin, 'r') as f:
#         for l in f:
#             l = unicode(l.strip('\n'))
#             if l:
#                 items.append(l)
#     return items


##############################
import jieba
import jieba.posseg as pseg
import os
from operator import add
from global_config import get_root_path

medical_pos = 'med'
user_dict_path = os.path.join(get_root_path(), 'medical_vocabulary_pos')
# all_medical_words = load_lines(user_dict_path)
# user_dict_path = 'medical_vocabulary_pos'
input_file = "/user/classify/ctr_train_data/raw_qa_ask_test"
clinic_cnt_file = "/user/classify/ctr_train_data/clinic_no_cnt"
w_c_key_cnt_file = "/user/classify/ctr_train_data/w_c_key_cnt"
jieba.load_userdict(user_dict_path)

# {"ask": "\u6307\u7532\u611f\u67d3\u4e86\uff0c\u5e94\u5f53\u62b9\u4ec0\u4e48\u836f\u5462\uff1f\uff08\u7537\uff0c24\u5c81\uff09", "clinic_no": "4", "key": "2017-01-01_10025978_471521415"}
def kernel1(iter):

    # jieba.load_userdict(user_dict_path)

    for line in iter:
        line = json.loads(line.strip('\n'))
        text = line['ask']
        clinic_no = line['clinic_no']
        tags = []
        word_pos_list = [x for x in pseg.cut(text)]
        for word, pos in word_pos_list:
            if pos != medical_pos:
                continue
            tags.append(word+'|||'+clinic_no)

        yield tags






def kernel2(line):
    line = json.loads(line.strip('\n'))
    clinic_no = line['clinic_no']
    return clinic_no


def main1():
    rdd1 = sc.textFile(input_file)
    keys_lines = rdd1.mapPartitions(kernel1).flatMap(lambda x: x).filter(lambda x: len(x) > 0).map(lambda x: (x, 1))
    clinic_lines = rdd1.map(kernel2).map(lambda x: (x, 1))
    keys_count = keys_lines.reduceByKey(add)
    clinic_count = clinic_lines.reduceByKey(add)

    a = keys_count.take(10)
    b = clinic_count.take(10)
    print a
    print b


main1()
