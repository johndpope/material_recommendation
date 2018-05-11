#encoding=utf8

import sys
import time
import json
from random import shuffle

from rpc_thrift.utils import get_fast_transport, get_service_protocol
from material_recommendation_service.MaterialRecommendationService import Client
from problem_triage_service.ProblemTriageService import Client as PT_client
from search.SearchService import Client as Search_client

RPC_LOCAL_PROXY = "10.215.33.5:5550"  # 线上
# RPC_LOCAL_PROXY = "10.9.89.126:5550"  # test_biz

service = "material_recommendation_service"

def test_rn_by_uid():

    uid = 116967326
    num = 2

    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service, fast=True)
    client = Client(protocol)

    input_arg = json.dumps(
        {
            'user_id': uid,
            'top_n': num
        }
    )

    o = json.loads(client.recommend_news(input_arg))
    print 'ids',o['ids']

def test_problem_triage():
    from problem_triage_service.ttypes import TriageParams
    service1 = 'problem_triage'
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service1, fast=True)
    client = PT_client(protocol)
    query = '感冒'

    result = client.triage_second(
        TriageParams(content=query, has_image=None, problem_id=None))

    print result

def test_search_doctor():
    input_arg = json.dumps({"filter": {"is_tel_price_v2": 0,"clinic_no":"ag",},
                            "query": {"start": "0",
                                      "text": "妇科", "rows": "30", "partner_name": "qiangshengxinpuni"},
                            "sort": "default"})

    service1 = 'search'
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service1, fast=True)
    client = Search_client(protocol)

    o = json.loads(client.search_doctors(input_arg))
    for x in o:print x


def test_get_doctor_topn_diseases():
    service1 = 'search'
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service1, fast=True)
    client = Search_client(protocol)
    id1 = "clinic_web_c383b3a7e6db1f1d"
    id2 = "clinic_web_1031d90f8b4de6aa"
    o = client.get_doctor_topN_diseases(id1)
    print o

def test_big_search():
    q = '哺乳期间'
    input_arg = json.dumps({"query":
                                {"province": "\u5b81\u590f\u56de\u65cf\u81ea\u6cbb\u533a",
                                 "has_topic": False, "text": q, "has_video": False,
                                 "is_force": 0, "ip": "111.111.111.111", "uid": "144178157",
                                 "device_id": "dc4cf9c2ef89ef9"}})
    service1 = 'search'
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service1, fast=True)
    client = Search_client(protocol)

    o = client.big_search(input_arg)
    print o



def test_big_search3():
    with open("some_query.txt","r") as f:
        lines = f.readlines()

    qs = [item.split('|')[-1].strip('\n') for item in lines]
    for q in qs:
        test_big_search3_kernel(q)


def test_big_search3_kernel(q):
    input_arg = json.dumps({"query":
                                {"province": "\u5b81\u590f\u56de\u65cf\u81ea\u6cbb\u533a",
                                 "has_topic": False, "text": q, "has_video": False,
                                 "is_force": 0, "ip": "111.111.111.111", "uid": "144178157",
                                 "device_id": "dc4cf9c2ef89ef9","clinic_no":"2"}})
    service1 = 'search'
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service1, fast=True)
    client = Search_client(protocol)

    o = client.big_search3(input_arg)
    print o


test_search_doctor()
test_get_doctor_topn_diseases()
test_big_search()

# test_big_search3()

