# encoding=utf8

import time
from general_utils import pysolr
from general_utils.solr_utils import SolrQuery
from general_utils.time_utils import ensure_m_timestamp, timestamp2datetime

zk_md4 = pysolr.ZooKeeper('md4:2181',
                          ['user_profile', 'news_profile', 'search_event', 'topic_profile',
                           'news_and_topic', 'simple_medical_entity'])
zk_md7 = pysolr.ZooKeeper('md7:2181,md8:2181,md9:2181', ['topic_tpl',
                                                         'topics', 'robot_news',
                                                         'user_topn_topics',
                                                         'news_and_topic',
                                                         'full_problem'])

solr_np = pysolr.SolrCloud(zk_md4, 'news_profile', timeout=15)  # news_profile
solr_tp = pysolr.SolrCloud(zk_md4, 'topic_profile', timeout=15)  # topic_profile
solr_nat = pysolr.SolrCloud(zk_md7, 'news_and_topic', timeout=15)  # news_and_topic

# def test():
#     import csv
#     from general_utils.text_utils import convert2gbk
#     fo = open('very_horrible_news.csv','w')
#     csvwriter = csv.writer(fo)
#     first_line = ['id','type','title']
#     csvwriter.writerow(first_line)
#
#     solr_query = SolrQuery()
#     q = 'query_text:自杀 OR query_text:跳楼 OR query_text:死'
#     solr_query.set('q', q)
#     solr_query.set('rows', 1000)
#     solr_query.set('fl', ['id', 'title'])
#     for item in solr_nat.search(**solr_query.get_query_dict()):
#         id = item['id']
#         title = item['title']
#         type = 'news' if 'news' in id else 'topic'
#         line = convert2gbk([str(id),type,title])
#         csvwriter.writerow(line)
#     fo.close()


every_day_topic_cnt = 10000
every_day_news_cnt = 26666


def get_view_profile_rowkeys(begin, end, mode='news'):
    # topic一个月大约30w，news一个大约80w，不需要快
    print 'begin', begin, timestamp2datetime(begin)
    print 'end', end, timestamp2datetime(end)

    # 确定solr表
    solr = solr_np if mode == 'news' else solr_tp

    # 调整时间戳为毫秒格式
    begin = ensure_m_timestamp(begin)
    end = ensure_m_timestamp(end)
    print begin
    print end

    # 估算rows
    days = (end - begin) / 86400 / 1000
    rows = days * every_day_topic_cnt if mode == 'topic' else days * every_day_news_cnt
    print 'rows', rows
    # 构建索引
    solr_query = SolrQuery()
    q = '*:*'
    solr_query.set('q', q)
    solr_query.add('fq', 'event_time:[%s TO %s]' % (begin, end))
    solr_query.set('rows', rows)
    solr_query.set('fl', ['id'])

    rowkey_list = [item['id'] for item in solr.search(**solr_query.get_query_dict())]
    print len(rowkey_list)
    return rowkey_list


def test():
    end = time.time()
    begin = end - 86400 * 30
    t1 = time.time()
    get_news_profile_rowkeys(begin, end)
    t2 = time.time()
    print 'time', t2 - t1


if __name__ == '__main__':
    test()
