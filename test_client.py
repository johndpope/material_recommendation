# encoding=utf8

from __future__ import absolute_import
import sys
import time
import json
from random import shuffle

from rpc_thrift.utils import get_fast_transport, get_service_protocol
from material_recommendation_service.MaterialRecommendationService import Client

from general_utils.solr_utils import nat_get_title
from general_utils.hbase_utils import user_last_query

RPC_LOCAL_PROXY = "10.215.33.5:5550"  # 线上
RPC_LOCAL_PROXY = "10.9.89.126:5550"  # test_biz

service = "material_recommendation_service"


def ar():
    print "=========ar=========="
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service, fast=True)
    client = Client(protocol)

    input = json.dumps(
        [{'user_id': 2585377, 'timestamp': 1512375821.56}])

    output = client.article_recommend(input)
    print output
    return output


def test_recommend_tags():
    print "=========test_recommend_tags=========="
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service, fast=True)
    client = Client(protocol)
    input = json.dumps(
        {'user_id': 112885379})
    output = client.recommend_tags(input)
    output = json.loads(output)['output']
    print output
    plan = output['plan']
    words = output['words']
    for x in plan:
        print 'plan', x['name'], x['url']
    for x in words:
        print 'word', x


def test_recommend_topn_topic():
    print "=========test_recommend_topn_topic=========="
    # from general_utils.solr_utils import nat_get_title
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service, fast=True)
    client = Client(protocol)
    input = json.dumps(
        {'user_id': 112885379})
    output = client.recommend_topn_topic(input)
    output = json.loads(output)['output']
    print output


def test_recommend_list():
    print "=========test_recommend_list=========="
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service, fast=True)
    client = Client(protocol)
    input = json.dumps(
        {'user_id': -1})
    output = client.recommend_list(input)
    print output


def test_many():
    file_name = sys.argv[2]
    num = int(sys.argv[3])
    qs = []
    uids = set()
    with open(file_name, 'r') as f:
        for l in f:
            ll = l.strip('\n').split(',')
            # print ll
            if len(ll) != 12:
                continue
            if ll[0] == 'uid':
                continue
            uid = int(ll[0])
            if uid in uids:
                continue
            uids.add(uid)
            ts = float(ll[5])
            info = ll[4].decode('gbk')
            qs.append([uid, ts, info])
    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service, fast=True)
    client = Client(protocol)

    times_ar = {}
    times_topic = {}
    times_tags = {}
    times_list = {}
    times_news = {}
    ar_ecps = []
    rl_ecps = []
    # begin = True

    print len(qs)
    shuffle(qs)

    for uid, ts, info in qs[:num]:
        time.sleep(1)  # 防止接口累着
        # if uid == 3024070:
        #     begin = True
        # if not begin:
        #     continue
        #  test article_recommend
        input = json.dumps(
            [{'user_id': uid, 'timestamp': ts}])
        t1 = time.time()
        if True:
            # try:
            output = client.article_recommend(input)
        # except Exception, e:
        #     ar_ecps.append([uid, e])e

        t2 = time.time()
        times_ar[uid] = t2 - t1
        print "========ar============"
        print uid, ts, info
        try:
            print json.loads(output)["output"][0]["title"]
        except:
            pass

        # test recommend_topn_topcs
        input = json.dumps(
            {'user_id': uid}
        )
        t1 = time.time()
        output = json.loads(client.recommend_topn_topic(input))['output']
        t2 = time.time()
        times_topic[uid] = t2 - t1
        if output:
            print "==========recommend topics========"
            for id in output:
                title = nat_get_title('topic_' + str(id))
                print uid, id, title

        # test recommend_list
        input = json.dumps(
            {'user_id': uid,
             'timestamp': ts}
        )

        t1 = time.time()
        if True:
            # try:
            output = client.recommend_list(input)
        # except Exception,e:
        #     output = json.dumps({'output':[]})
        #     rl_ecps.append([uid,e])
        t2 = time.time()
        times_list[uid] = t2 - t1

        output = json.loads(output)['output']
        if output:
            print "========recommend_list=========="
            for item in output:
                print uid, item['id'], item['type'], item['title']

        # test recommend tags
        input = json.dumps(
            {
                'user_id': uid,

            }
        )
        t1 = time.time()
        output = client.recommend_tags(input)
        t2 = time.time()
        times_tags[uid] = t2 - t1
        output = json.loads(output)['output']
        words = output['words']
        plan = output['plan']
        print "=======recommend tags=========="
        print uid
        last_query = user_last_query(uid)
        print "last_query", last_query
        print "words", '-'.join(words)
        for item in plan:
            print item['name'], item['url']

        # test recommend_news
        input = json.dumps(
            {
                'user_id': uid,
                'top_n': 2
            }
        )

        t1 = time.time()
        output = client.recommend_news(input)
        t2 = time.time()
        times_news[uid] = t2 - t1
        output = json.loads(output)
        ids = output['ids']
        titles = [nat_get_title('news_' + str(id)) for id in ids]
        print "=======recommend news=========="
        print uid
        for i, id in enumerate(ids):
            print id, titles[i]

    print "mean time ar", sum(times_ar.values()) / len(times_ar)
    s_times = sorted(times_ar.iteritems(), key=lambda x: x[1], reverse=True)
    for uid, t in s_times[:10]:
        print uid, t

    print '---------'

    print "mean time recommend topic", sum(times_topic.values()) / len(times_topic)
    s_times = sorted(times_topic.iteritems(), key=lambda x: x[1], reverse=True)
    for uid, t in s_times[:10]:
        print uid, t

    print '---------'

    print "mean time recommend list", sum(times_list.values()) / len(times_list)
    s_times = sorted(times_list.iteritems(), key=lambda x: x[1], reverse=True)
    for uid, t in s_times[:10]:
        print uid, t

    print '---------'

    print "mean time recommend tags", sum(times_tags.values()) / len(times_tags)
    s_times = sorted(times_tags.iteritems(), key=lambda x: x[1], reverse=True)
    for uid, t in s_times[:10]:
        print uid, t

    print '---------'

    print "mean time recommend news", sum(times_news.values()) / len(times_news)
    s_times = sorted(times_news.iteritems(), key=lambda x: x[1], reverse=True)
    for uid, t in s_times[:10]:
        print uid, t

    print '---------'

    for u, e in ar_ecps:
        print " ar exceptions", u, e

    for u, e in rl_ecps:
        print "rl exceptions", u, e


def get_one_day_uid_from_file(file_name):
    uids = set()
    with open(file_name, 'r') as f:
        for l in f:
            uid = l.split('|')[0]
            uids.add(int(uid))
    return list(uids)


def test_recommend_plan():
    test_num = int(sys.argv[2])

    endpoint = RPC_LOCAL_PROXY

    get_fast_transport(endpoint)

    protocol = get_service_protocol(service, fast=True)
    client = Client(protocol)

    from random import shuffle
    uids = get_one_day_uid_from_file('log_event_20180122')
    shuffle(uids)
    selected_uids = uids[:test_num]

    total_times = {}
    for uid in selected_uids:
        input_arg = json.dumps(
            {
                'user_id': uid,
                'top_n': 2
            }
        )
        t1 = time.time()
        o = client.recommend_plan(input_arg)
        t2 = time.time()
        total_times[uid] = t2 - t1
        print '==' * 10
        print uid
        print o

    sorted_total_times = sorted(total_times.iteritems(), key=lambda x: x[1], reverse=True)
    for uid, t in sorted_total_times[:10]:
        print uid, t


def test_rn_by_uid():

    uid = sys.argv[2]
    num = int(sys.argv[3])

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



if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'many':
        test_many()
    elif mode == 'rn':
        test_rn_by_uid()
    elif mode == 'rp':
        test_recommend_plan()
    # test_recommend_plan()
    # ar()
    # test_recommend_tags()
    # test_recommend_topn_topic()
    # test_recommend_list()
