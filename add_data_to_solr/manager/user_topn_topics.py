# encoding=utf8
'''
将前一天的活跃用户的推荐的医生话题离线计算后，存入线上solr的user_topn_topics表
'''

import sys
import time
import json
import random

from add_data_to_solr.cy_solr_local.solr_base import SolrCloud, ZooKeeper
from add_data_to_solr.manager.add_utils import add_all

from general_utils.time_utils import timestamp2date
from recommend.daily_scripts.utils import get_parti_solr_filename

solr = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "user_topn_topics")


def test_insert():
    '''
    id:string date|uid
    topic_ids: string json.dumps([1,2,3])
    :return:
    '''
    date = timestamp2date(time.time() - 86400.0)

    topic_ids = [1, 2, 3, 5555, 666]
    docs = []
    uids = set()
    for i in range(10000):
        uid = random.randint(1, 99999999)
        if uid in uids:
            continue
        uids.add(uid)
        key = date + '|' + str(uid)
        docs.append(
            {
                'id': key,
                'topic_ids': json.dumps(topic_ids),
                'timestamp': int(time.time() * 1000),
            }
        )
    add_all(docs, solr)


def add_a_part(part):
    # {"topics": [], "key": "20180101|135495695"}
    file_name = get_parti_solr_filename(part, 'topic')
    docs = []
    with open(file_name, 'r') as f:
        for l in f:
            info_dict = json.loads(l.strip('\n'))
            key = info_dict['key']
            topics = info_dict['topics']
            # if not topics:
            #     continue
            docs.append(
                {
                    'id': key,
                    'topic_ids': json.dumps(topics),
                    'timestamp': int(time.time() * 1000),
                }
            )
    add_all(docs, solr)


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'test':
        test_insert()
    elif mode == 'add':
        part = sys.argv[2]
        add_a_part(part)
