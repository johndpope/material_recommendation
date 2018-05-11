# encoding=utf8
from __future__ import absolute_import
import json
from rpc_services import rpc_clients
from chunyu.utils.general.encoding_utils import ensure_unicode


def more_news(query):
    return rpc_clients.get_search_service_client().more_news(query)


def more_topic(query, rows=30):
    '''
    :param  query_str json_dict_str of {query(关键词), start(开始索引), rows(话题数)}
    :return {result:[{id, title}]}
    '''
    input = json.dumps({"query": query, "rows": rows})

    return json.loads(rpc_clients.get_search_service_client().more_topics(input))['result']


def get_news_from_bigsearch(query):
    query = ensure_unicode(query)
    return [int(x) for x in json.loads(more_news(query))[0]["ids"]]


def get_topic_from_bigsearch(query, rows=30):
    # query = ensure_unicode(query)
    bad_return = [], {}
    # if not query:
    #     return bad_return
    title_dict = {}
    recall_id = []
    for item in more_topic(query, rows):
        id = int(item['id'])
        title = item['title']
        if title:
            recall_id.append(id)
            title_dict[id] = title
    return recall_id, title_dict


def search_doctors(query,topn=50):
    input_arg = json.dumps({"filter": {"is_tel_price_v2": 0},
                            "query": {"start": "0",
                                      "text": query, "rows": str(topn)}, "sort": "default"})
    try:
        did_list = json.loads(rpc_clients.get_search_service_client().search_doctors(input_arg))
        return did_list
    except Exception, e:
        print "search_doctors exception", e
        return []


def get_doctor_topN_diseases(did):
    did = str(did)
    try:
        o = json.loads(rpc_clients.get_search_service_client().get_doctor_topN_diseases(did))['hot_consults']
        return [[item['keywords'], item['order_num']] for item in o]
    except Exception, e:
        print "get_doctor_topN_diseases exception", e
        return []


def test_search_doctors():
    # input_arg = '{"filter":{"is_tel_price_v2":1},"query":{"province":"北京市","city":"北京市","ip":"61.135.165.43","start":"0","text":"单发性骨髓瘤","rows":"20"},"sort":"default"}'
    input_arg = json.dumps({"filter": {"is_tel_price_v2": 0},
                            "query": {"start": "0",
                                      "text": "辅舒良", "rows": "50","partner_name":"qiangshengxinpuni"}, "sort": "default"})
    o = rpc_clients.get_search_service_client().search_doctors(input_arg)
    o = json.loads(o)
    for x in o:
        print x


def test_get_doctor_topN_diseases():
    o = rpc_clients.get_search_service_client().get_doctor_topN_diseases("asdads")
    o = json.loads(o)['hot_consults']
    print o
    for item in o:
        print item['keywords'], item['order_num']



if __name__ == '__main__':
    test_search_doctors()