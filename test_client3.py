# encoding=utf8
'''
用于测试服务的所有接口
'''

import sys
import time
import json
from random import shuffle

from rpc_thrift.utils import get_fast_transport, get_service_protocol
from material_recommendation_service.MaterialRecommendationService import Client
from problem_triage_service.ProblemTriageService import Client as PT_client
from search.SearchService import Client as Search_client

RPC_LOCAL_PROXY_BIZ = "10.9.89.126:5550"
RPC_LOCAL_PROXY = "10.215.33.5:5550"


class TestBigSearch(object):
    def __init__(self, ForTest=False):
        service = 'search'
        if ForTest:
            endpoint = RPC_LOCAL_PROXY_BIZ
        else:
            endpoint = RPC_LOCAL_PROXY

        get_fast_transport(endpoint)

        protocol = get_service_protocol(service, fast=True)
        self.client = Search_client(protocol)

    def test_search_doctors(self):
        input_arg = json.dumps({"filter": {"is_tel_price_v2": 0},
                                "query": {"start": "9",
                                          "text": "妇科", "rows": "20", "partner_name": "qiangshengxinpuni"},
                                "sort": "default"})
