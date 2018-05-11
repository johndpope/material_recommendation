# encoding=utf8
from __future__ import absolute_import
import numpy as np
import json
from rpc_thrift.utils import get_fast_transport, get_service_protocol
from cy_word2vec_service.Word2VecService import Client as Word2vec_client

RPC_LOCAL_PROXY_BIZ = "10.9.89.126:5550"  # biz

default_timeout_w2v = 400

_client_word2vec_service = None


def get_word2vec_service_client():
    global _client_word2vec_service
    if not _client_word2vec_service:
        _ = get_fast_transport(RPC_LOCAL_PROXY_BIZ, timeout=default_timeout_w2v)
        protocol = get_service_protocol(service="cy_word2vec", fast=True)
        _client_word2vec_service = Word2vec_client(protocol)
    return _client_word2vec_service


def test_w2v():
    word_list = [u'鼻涕', u'清鼻涕', u'黄鼻涕', u'肺气肿', u'脑袋', u'']

    l = len(word_list)
    vecs = json.loads(get_word2vec_service_client().get_w2v_vector_of_words(json.dumps(word_list)))
    print type(vecs)
    for x in vecs:
        print x, len(vecs[x]), type(vecs[x])

    for i in range(l):
        for j in range(i, l):
            print '-' * 20
            w1 = word_list[i]
            w2 = word_list[j]
            print w1, w2
            v1 = vecs[w1]
            v2 = vecs[w2]
            if (not v1) or (not v2): continue
            v1 = np.array(v1)
            v2 = np.array(v2)

            print np.linalg.norm(v1)
            print np.linalg.norm(v2)
            print np.dot(v1, v2)


def test_similar():
    word_list = [u'鼻涕', u'清鼻涕', u'黄鼻涕', u'肺气肿', u'脑袋', u'甲亢']
    for w in word_list:
        print '-' * 20
        print w
        o = get_word2vec_service_client().get_w2v_most_similar(w, 50)
        o = json.loads(o)
        for x, y in o:
            print x, y


test_similar()
