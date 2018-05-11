# encoding=utf8

'''
task1 将很多（一个月）活动用户的推荐news id存入solr
task2 每日任务：将前一天活动用户的数据更新了
'''
import os
import sys
import time
import json
import random

from add_data_to_solr.cy_solr_local.solr_base import SolrCloud, ZooKeeper
from add_data_to_solr.manager.add_utils import add_all

from general_utils.time_utils import timestamp2date, ensure_m_timestamp
from recommend.daily_scripts.utils import get_parti_solr_filename

solr = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "user_topn_news")
solr_test = SolrCloud(ZooKeeper("rd1:2181,rd2:2181"), "user_topn_news")

def test_insert():
    '''
    id:string date|uid
    topic_ids: string json.dumps([1,2,3])
    :return:
    '''
    # date = timestamp2date(time.time() - 86400.0)

    topic_ids = [1, 2, 3, 5555, 666]
    docs = []
    uids = set()
    for i in range(10000):
        uid = random.randint(1, 99999999)
        if uid in uids:
            continue
        uids.add(uid)
        key = str(uid)
        docs.append(
            {
                'id': key,
                'news_ids': json.dumps(topic_ids),
                'timestamp': int(time.time() * 1000),
            }
        )
    add_all(docs, solr)


def from_file(file_name):
    '''
    从文件中读取数据；若没有last_event_time字段，则将其设置为当前
    :param file_name:
    :return:
    '''
    data = []
    with open(file_name, 'r') as f:
        for l in f:
            data.append(json.loads(l.strip('\n')))

    docs = []
    for item in data:
        now = time.time()
        uid = str(item['uid'])
        news_ids = json.dumps(item['ids'])
        last_event_time = ensure_m_timestamp(item.get('last_event_time', now))
        timestamp = ensure_m_timestamp(now)

        docs.append(
            {
                'id': uid,
                'news_ids': news_ids,
                'last_event_time': last_event_time,
                'timestamp': timestamp,
            }
        )

    add_all(docs, solr)


many_user_data_dir = '/home/classify/workspace/material_recommendation/recommend/data_dir/half_month_uids_split'


def many_users():
    files = []
    for file_name in os.listdir(many_user_data_dir):
        if not file_name.endswith('.ids'):
            continue
        files.append(os.path.join(many_user_data_dir, file_name))
    for x in files:
        from_file(x)
        print '%s is put into solr'%x


def add_a_part(part):
    # {"ids": [], "uid": "135495695",'last_event_time': 1519276793.12873}
    print 'start add part %s'%part
    file_name = get_parti_solr_filename(part,'news')
    add_a_part_kernel(file_name)


def add_a_part_kernel(file_name):
    print 'file name = %s' % file_name
    docs = []
    with open(file_name, 'r') as f:
        for l in f:
            info_dict = json.loads(l.strip('\n'))
            uid = info_dict['uid']
            news_ids = info_dict['ids']
            last_event_time = info_dict['last_event_time']  # 秒
            last_event_time = int(last_event_time * 1000)  # 毫秒

            docs.append(
                {
                    'id': str(uid),
                    'news_ids': json.dumps(news_ids),
                    'timestamp': int(time.time() * 1000),
                    'last_event_time': last_event_time,
                }
            )
    add_all(docs, solr)
    #add_all(docs, solr_test)


def add_a_dir(dirname):
    files = []
    for file_name in os.listdir(dirname):
        print file_name
        if  '.solr' not in file_name:
            continue
        files.append(os.path.join(dirname, file_name))
    for x in files:
        add_a_part_kernel(x)





if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'many':
        many_users()
    elif mode == 'daily':
        part = sys.argv[2]
        add_a_part(part)
    elif mode == 'all_d':
        dirname = sys.argv[2]
        print dirname
        add_a_dir(dirname)
