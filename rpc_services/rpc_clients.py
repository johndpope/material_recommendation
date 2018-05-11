# encoding=utf8
from __future__ import absolute_import

from rpc_thrift.utils import get_fast_transport, get_service_protocol
from medical_service.MedicalService import Client as Medical_client
from cy_word2vec_service.Word2VecService import Client as Word2vec_client
from search.SearchService import Client as Search_client
from problem_promotion_service.QAPromotionService import Client as QAPromotionClient
from problem_triage_service.ProblemTriageService import Client as ProblemTriageClient
from user_profile.UserProfileService import Client as UserProfileClient

RPC_LOCAL_PROXY = "10.215.33.5:5550"  # 线上
RPC_LOCAL_PROXY_BIZ = "10.9.89.126:5550"  # biz

default_timeout = 500  # 防止调用别的接口超时这里不能设置太大
default_timeout_w2v = 400
_client_medical_service = None
_client_qa_promotion_service = None
_client_word2vec_service = None
_client_search_service = None
_client_problem_triage_service = None
_client_user_profile = None


# _client_word2vec_service_biz = None


def get_medical_service_client():
    global _client_medical_service
    if not _client_medical_service:
        _ = get_fast_transport(RPC_LOCAL_PROXY, timeout=default_timeout)
        protocol = get_service_protocol(service='medical_service', fast=True)
        _client_medical_service = Medical_client(protocol)
    return _client_medical_service


def get_user_profile_client():
    global _client_user_profile
    if not _client_user_profile:
        _ = get_fast_transport(RPC_LOCAL_PROXY, timeout=default_timeout)
        protocol = get_service_protocol(service="user_profile", fast=True)
        _client_user_profile = UserProfileClient(protocol)
    return _client_user_profile


def get_problem_promotion_service_client():
    global _client_qa_promotion_service
    if not _client_qa_promotion_service:
        _ = get_fast_transport(RPC_LOCAL_PROXY, timeout=default_timeout)
        protocol = get_service_protocol(service="qa_promotion_service", fast=True)
        _client_qa_promotion_service = QAPromotionClient(protocol)
    return _client_qa_promotion_service


def get_word2vec_service_client():
    global _client_word2vec_service
    if not _client_word2vec_service:
        _ = get_fast_transport(RPC_LOCAL_PROXY, timeout=default_timeout_w2v)
        try:
            protocol = get_service_protocol(service="word2vec_service", fast=True)
        except:
            protocol = get_service_protocol(service="cy_word2vec", fast=True)

        _client_word2vec_service = Word2vec_client(protocol)
    return _client_word2vec_service


def get_problem_triage_client():
    global _client_problem_triage_service
    if not _client_problem_triage_service:
        _ = get_fast_transport(RPC_LOCAL_PROXY, timeout=default_timeout)
        protocol = get_service_protocol(service="problem_triage", fast=True)
        _client_problem_triage_service = ProblemTriageClient(protocol)
    return _client_problem_triage_service


# def get_word2vec_service_client_biz():
#     global _client_word2vec_service_biz
#     if not _client_word2vec_service_biz:
#         trainsport = get_fast_transport2(RPC_LOCAL_PROXY_BIZ, timeout=default_timeout)
#         protocol = get_service_protocol(transport=trainsport, service="word2vec_service", fast=True)
#         _client_word2vec_service_biz = Word2vec_client(protocol)
#     return _client_word2vec_service_biz


def get_search_service_client():
    global _client_search_service
    if not _client_search_service:
        _ = get_fast_transport(RPC_LOCAL_PROXY, timeout=default_timeout)
        protocol = get_service_protocol(service="search", fast=True)

        _client_search_service = Search_client(protocol)
    return _client_search_service
