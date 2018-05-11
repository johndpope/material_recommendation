# -*- coding:utf-8 -*-
import os
from global_config import get_root_path, get_data_path

RESOURCE_DATA_FILE = os.path.join(get_data_path(), 'resource_data.pickle')
TOPIC_SCORE_FILE = os.path.join(get_data_path(), 'topic_score.pickle')
TOPIC_DATA_FILE = os.path.join(get_data_path(), 'topic_data.pickle')
BODYPART_FILE = os.path.join(get_data_path(), 'bodypart.pickle')
MEDICAL_ENTITY_FILE = os.path.join(get_data_path(), 'medical_entity.pickle')
MEDICAL_RELATION_DRUG_FILE = os.path.join(get_data_path(), 'relation_drug.pickle')
SYSTAG_DATA_FILE = os.path.join(get_data_path(), 'systag_data.pickle')
SYSTAG_DATA_CHECK_FILE = os.path.join(get_data_path(), 'systag_data.check')

USER_TOPN_TOPIC_DATADIR = os.path.join(get_data_path(), 'user_topn_topic')
USER_TOPN_NEWS_DATADIR = os.path.join(get_data_path(), 'user_topn_news')


WORDS_FREQ_FILE = os.path.join(get_root_path(), 'recommend', 'data_dir', 'words_freq.txt')

RECOMMED_TAGS_KEEP_NUM = {
    u"DISEASE_DESC": 2,
    u"SYMPTOM_DESC": 1,
    u"DRUG_DESC": 2,
}

RECOMMED_TAGS_ORDER = [u"DISEASE_DESC", u"SYMPTOM_DESC", u"DRUG_DESC"]


def get_high_freq_words():
    hw = []
    with open(WORDS_FREQ_FILE,'r') as f:
        for l in f:
            ll = l.split('\t')
            hw.append(ll[1].decode('utf8'))
    return hw
