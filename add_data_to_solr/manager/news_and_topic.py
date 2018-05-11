# encoding=utf8

'''
将科普文章与医生话题放在一起索引
位于md4上的news_and_topic表
'''

import sys
import time

from add_data_to_solr.cy_solr_local.solr_base import SolrHelper, SolrCloud, ZooKeeper
from general_utils.db_utils import get_medicaldb_handler
from general_utils.db_utils import get_newsdb_handler
from general_utils.solr_utils import get_max_true_id, solr_nat, SolrQuery
from add_data_to_solr.manager.add_utils import add_all, update_all, topic_info, doctor_info, topic_info_big
from general_utils.file_utils import pickle_from_file, pickle_to_file
from recommend.app_config import TOPIC_SCORE_FILE

solr = SolrCloud(ZooKeeper("md4:2181"), "news_and_topic")
solr_new = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "news_and_topic")

solr_test = SolrCloud(ZooKeeper("rd1:2181,rd2:2181"), "news_and_topic")


def add_news():
    """
    fields:
    id:"news_123123"
    true_id:123123
    title:"dhsajsdhaj"
    digest:"rrsdfsfds"
    query_text:title+digest
    news_type:"SH"
    type:"news"
    老快了！！！！！
    """
    id_prefix = "news_"
    sql = "select id,title,digest,news_type from news_healthnews where is_online=1;"
    o = get_newsdb_handler().do_one(sql)
    docs = []

    for item in o:
        id = item[0]
        title = item[1]
        if not title:
            continue

        digest = item[2]
        news_type = item[3]
        docs.append(
            {
                "id": id_prefix + str(id),
                "true_id": int(id),
                "title": title,
                "digest": digest,
                "query_text": u' '.join([title, digest]),
                "news_type": news_type,
                "type": "news",
                "timestamp": int(time.time() * 1000)
            }
        )

    # solr = SolrCloud(ZooKeeper("md4:2181"), "news_and_topic")
    add_all(docs, solr_new)


def add_news_biztest():
    """
    fields:
    id:"news_123123"
    true_id:123123
    title:"dhsajsdhaj"
    digest:"rrsdfsfds"
    query_text:title+digest
    news_type:"SH"
    type:"news"
    老快了！！！！！
    """
    id_prefix = "news_"
    sql = "select id,title,digest,news_type from news_healthnews where is_online=1;"
    o = get_newsdb_handler().do_one(sql)
    docs = []

    for item in o:
        id = item[0]
        title = item[1]
        if not title:
            continue

        digest = item[2]
        news_type = item[3]
        docs.append(
            {
                "id": id_prefix + str(id),
                "true_id": int(id),
                "title": title,
                "digest": digest,
                "query_text": u' '.join([title, digest]),
                "news_type": news_type,
                "type": "news",
                "timestamp": int(time.time() * 1000)
            }
        )

    # solr = SolrCloud(ZooKeeper("md4:2181"), "news_and_topic")
    add_all(docs, solr_test)


def add_topic():
    '''
    fields:
    id:"topic_2222"
    true_id:2222
    title:"wuyetruyw"
    query_text:title
    type:"topic"
    clinic_no:"1"
    second_class_clinit_no:"fa"
    topic_score:90
    老慢了
    '''

    old_topic_score = pickle_from_file(TOPIC_SCORE_FILE)
    id_prefix = "topic_"
    sql = 'select id from api_doctortopic where is_deleted=0 and title <> "";'
    o = get_medicaldb_handler().do_one(sql)

    docs = []

    for item in o:
        id = int(item[0])
        if id in old_topic_score:
            info_of_topic = topic_info(id)
            doctor_id = info_of_topic['doctor_id']
            info_of_doctor = doctor_info(doctor_id)
            score = old_topic_score[id]
        else:
            score, info_of_topic, info_of_doctor = topic_info_big(id)

        # info_of_topic = topic_info(id)
        # doctor_id = info_of_topic['doctor_id']
        title = info_of_topic['title'].strip()
        if len(title) == 0:
            continue
        # info_of_doctor = doctor_info(doctor_id)
        clinic_no = info_of_doctor['first_class_clinic_no']
        second_class_clinic_no = info_of_doctor['second_class_clinic_no']
        docs.append(
            {
                "id": id_prefix + str(id),
                "true_id": id,
                "title": title,
                "query_text": title,
                "type": "topic",
                "clinic_no": clinic_no,
                "second_class_clinic_no": second_class_clinic_no,
                "topic_score": int(score * 10),
                "timestamp": int(time.time() * 1000)
            }
        )
    # solr = SolrCloud(ZooKeeper("md4:2181"), "news_and_topic")
    add_all(docs, solr_new)


def add_new_news():
    # get max news id
    max_news_id = get_max_true_id('news')
    print max_news_id
    id_prefix = "news_"
    sql = "select id,title,digest,news_type from news_healthnews where is_online=1 and id > %s;" % max_news_id
    o = get_newsdb_handler().do_one(sql)
    docs = []

    for item in o:
        id = item[0]
        title = item[1]
        if not title:
            continue

        digest = item[2]
        news_type = item[3]
        docs.append(
            {
                "id": id_prefix + str(id),
                "true_id": int(id),
                "title": title,
                "digest": digest,
                "query_text": u' '.join([title, digest]),
                "news_type": news_type,
                "type": "news",
                "timestamp": int(time.time() * 1000)
            }
        )
    for doc in docs:
        print '======='
        print doc['id'], doc['title'], doc['digest'], doc['news_type']
    # solr = SolrCloud(ZooKeeper("md4:2181"), "news_and_topic")
    add_all(docs, solr_new)


def add_new_topic():
    max_topic_id = get_max_true_id('topic')
    id_prefix = "topic_"
    sql = 'select id from api_doctortopic where is_deleted=0 and title <> "" and id > %s;' % max_topic_id
    o = get_medicaldb_handler().do_one(sql)
    old_topic_score = {}
    docs = []

    for item in o:
        id = int(item[0])
        if id in old_topic_score:
            info_of_topic = topic_info(id)
            doctor_id = info_of_topic['doctor_id']
            info_of_doctor = doctor_info(doctor_id)
            score = old_topic_score[id]
        else:
            score, info_of_topic, info_of_doctor = topic_info_big(id)

        # info_of_topic = topic_info(id)
        # doctor_id = info_of_topic['doctor_id']
        title = info_of_topic['title'].strip()
        if len(title) == 0:
            continue
        # info_of_doctor = doctor_info(doctor_id)
        clinic_no = info_of_doctor['first_class_clinic_no']
        second_class_clinic_no = info_of_doctor['second_class_clinic_no']
        docs.append(
            {
                "id": id_prefix + str(id),
                "true_id": id,
                "title": title,
                "query_text": title,
                "type": "topic",
                "clinic_no": clinic_no,
                "second_class_clinic_no": second_class_clinic_no,
                "topic_score": int(score * 10),
                "timestamp": int(time.time() * 1000)
            }
        )
    add_all(docs, solr_new)
    for doc in docs:
        print doc['id'], doc['title'], doc['topic_score']


####################迁移数据用的函数######################
def get_all_id():
    solrquery = SolrQuery()
    q = '*:*'
    solrquery.set('q', q)
    solrquery.set('rows', 100000)  # 八万多
    solrquery.set('fl', ['id'])
    res = [item['id'] for item in solr_nat.search(**solrquery.get_query_dict())]
    with open('all_news_and_topic_ids', 'w') as f:
        f.write('\n'.join(res))


def get_all_docs():
    docs = []
    cnt = 0
    with open('all_news_and_topic_ids', 'r') as f:
        for l in f:
            if cnt > 10:
                continue
            cnt += 1

            id = l.strip('\n')
            solrquery = SolrQuery()
            q = '*:*'
            solrquery.set('q', q)
            solrquery.set('rows', 1)
            solrquery.add('fq', 'id:%s' % id)
            solrquery.set('fl', ['*'])
            res = [item for item in solr_nat.search(**solrquery.get_query_dict())][0]
            del res['_version_']
            docs.append(res)
    add_all(docs, solr_new)


def get_topic_score():
    topic_score = {}  # key is int,score is float
    with open('all_news_and_topic_ids', 'r') as f:
        for l in f:
            id = l.strip('\n')
            if 'topic' not in id:
                continue

            solrquery = SolrQuery()
            q = '*:*'
            solrquery.set('q', q)
            solrquery.set('rows', 1)
            solrquery.add('fq', 'id:%s' % id)
            solrquery.set('fl', ['*'])
            res = [item.get('topic_score', 0) for item in solr_nat.search(**solrquery.get_query_dict())][0]
            topic_score[int(id.split('_')[1])] = res / 10.0

    pickle_to_file(topic_score, TOPIC_SCORE_FILE)


#####################################################
# fix bug
def get_all_id2():
    last_ts = 1516343674073
    solrquery = SolrQuery()
    q = '*:*'
    solrquery.set('q', q)
    solrquery.set('rows', 100000)  # 八万多
    solrquery.set('fl', ['id'])
    solrquery.add('fq', 'timestamp:[%s TO *]' % last_ts)
    res = [item['id'] for item in solr.search(**solrquery.get_query_dict())]
    with open('all_news_and_topic_ids', 'w') as f:
        f.write('\n'.join(res))


def fix1():
    '''
    将md4 new_and_topic中数据迁移到线上solr news_and_topic里（线上没有的数据）
    :return:
    '''
    docs = []
    cnt = 0
    with open('all_news_and_topic_ids', 'r') as f:
        for l in f:
            # if cnt > 10:
            #     continue
            # cnt += 1

            id = l.strip('\n')
            solrquery = SolrQuery()
            q = '*:*'
            solrquery.set('q', q)
            solrquery.set('rows', 1)
            solrquery.add('fq', 'id:%s' % id)
            solrquery.set('fl', ['*'])
            res = [item for item in solr.search(**solrquery.get_query_dict())][0]
            del res['_version_']
            mtype = res['type']
            if mtype == 'news':
                res['query_text'] = res['title'] + ' ' + res['digest']
            elif mtype == 'topic':
                res['query_text'] = res['title']
            docs.append(res)
    for x in docs:
        print x
    add_all(docs, solr_test)


horrible_words = [u'自杀', u'跳楼', u'死']


def is_horrible_text(text):
    try:
        text = text.decode('utf8', 'ignore')
    except:
        pass
    for x in horrible_words:
        if x in text:
            return True
    return False


def fix2_biztest():
    id_filename = 'all_news_and_topic_ids_biztest'
    solrquery = SolrQuery()
    q = '*:*'
    solrquery.set('q', q)
    solrquery.set('rows', 100000)  # 八万多
    solrquery.set('fl', ['id'])
    res = [item['id'] for item in solr_test.search(**solrquery.get_query_dict())]
    with open(id_filename, 'w') as f:
        f.write('\n'.join(res))

    #
    docs = []
    with open(id_filename, 'r') as f:
        for l in f:
            id = l.strip('\n')
            print id
            solrquery = SolrQuery()
            q = '*:*'
            solrquery.set('q', q)
            solrquery.set('rows', 1)
            solrquery.add('fq', 'id:%s' % id)
            solrquery.set('fl', ['title','digest'])
            res = [[item['title'],item.get('digest','')] for item in solr_test.search(**solrquery.get_query_dict())][0]
            title = res[0]
            digest = res[1]
            query_text = title + ' ' + digest
            is_horrible = 1 if is_horrible_text(title) else 0
            docs.append(
                {
                    'id': id,
                    'is_horrible': is_horrible,
                    'query_text':query_text,
                }
            )

    update_all(docs, solr=solr_test,fieldupdate_dict={"is_horrible": "set"})


def fix2():
    id_filename = 'all_news_and_topic_ids'
    solrquery = SolrQuery()
    q = '*:*'
    solrquery.set('q', q)
    solrquery.set('rows', 100000)  # 八万多
    solrquery.set('fl', ['id'])
    res = [item['id'] for item in solr_new.search(**solrquery.get_query_dict())]
    with open(id_filename, 'w') as f:
        f.write('\n'.join(res))

    #
    docs = []
    cnt = 0
    with open(id_filename, 'r') as f:
        for l in f:
            # if cnt > 10:
            #     break
            cnt += 1
            id = l.strip('\n')
            print id
            solrquery = SolrQuery()
            q = '*:*'
            solrquery.set('q', q)
            solrquery.set('rows', 1)
            solrquery.add('fq', 'id:%s' % id)
            solrquery.set('fl', ['title', 'digest'])
            res = [[item['title'], item.get('digest', '')] for item in solr_new.search(**solrquery.get_query_dict())][
                0]
            title = res[0]
            digest = res[1]
            query_text = title + ' ' + digest
            is_horrible = 1 if is_horrible_text(title) else 0
            docs.append(
                {
                    'id': id,
                    'is_horrible': is_horrible,
                    'query_text': query_text,
                }
            )

    update_all(docs, solr=solr_new, fieldupdate_dict={"is_horrible": "set"})


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'update':
        add_new_news()
        add_new_topic()
        # add_news()
        # add_topic()
    elif mode == 'test':
        fix2()
