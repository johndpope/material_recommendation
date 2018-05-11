# encoding=utf8


import json

from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(hosts=['10.215.33.36:9200'])

topic0 = "wechat_bot_big_search"
topic1 = "wechat_bot_qa_search"
topic2 = "wechat_bot_qa_detail"  # 每条记录都是点击




def get_some_query():
    data = get_log_lines(topic0)
    with open('some_query','w') as f:

        for one_record in data:
            source = one_record['_source']
            ip = source['ip']
            ts = source['@timestamp']
            query = source['query']
            stri = '|'.join([ip,ts,query])+'\n'
            f.write(stri)



def get_log_lines(topic):
    query = {
        "query": {
            "match": {
                "tag": {
                    "query": topic,
                    "type": "phrase"
                }
            }
        }
    }
    generator = helpers.scan(es, query=query, index='search.log-2018.05.06')
    cnt = 0

    # all_types = set()
    res = []
    for one_record in generator:
        res.append(one_record)

    return res


def get_from_url(url_str, key):
    return url_str.split(key + '=')[1].split('&')[0]


def parse_topic0(one_record):
    source = one_record['_source']
    ip = source['ip']
    ts = source['@timestamp']
    query = source['query']
    result = source['result']
    problem_list = result['problem_list']

    key = ip + '|' + ts
    plist = []
    for item in problem_list:
        ask = item['ask']
        id = item['id']
        problem_id = get_from_url(item['url'], 'problem_id')
        plist.append(id+'|'+problem_id)

    return key,plist,query



def parse_topic1(one_record):
    pass




def main():
    '''
    从wechat_bot_big_search取ip，@timestamp, 问题url中id，id,ask
    :return:
    '''
    for topic in (topic0, topic1, topic2):
        print "=" * 20
        print topic
        get_log_lines(topic)


def test1():
    '''

    :return:
    '''
    show_list_file = 'show_list'  # 按照时间戳排序的
    more_qa_list_file = 'more_qa_list'  # 按时间戳排序
    click_list_file = 'click_qa_list' # 按时间戳排序


    show_log_lines = get_log_lines(topic0)
    with open(show_list_file,'w') as f:
        for one_record in show_log_lines:
            key,plist,query = parse_topic0(one_record)
            stri = key + '|||' + query + '|||'.join(plist) + '\n'
            f.write(stri)

    more_qa_lines = get_log_lines(topic1)
    with open(more_qa_list_file,'w') as f:
        pass



if __name__ == '__main__':
    get_some_query()
