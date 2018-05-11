# encoding=utf8
import sys
import re
import json
from general_utils import pysolr
from general_utils.time_utils import ensure_second_timestamp, get_yesterday_date
from chunyu.utils.general.encoding_utils import ensure_unicode
from general_utils.text_utils import remove_other

zk_md4 = pysolr.ZooKeeper('md4:2181',
                          ['user_profile', 'news_profile', 'search_event', 'topic_profile',
                           'news_and_topic', 'simple_medical_entity'])
zk_md7 = pysolr.ZooKeeper('md7:2181,md8:2181,md9:2181', ['topic_tpl',
                                                         'topics', 'robot_news',
                                                         'user_topn_topics',
                                                         'news_and_topic',
                                                         'full_problem',
                                                         'user_topn_news'])

solr_up = pysolr.SolrCloud(zk_md4, 'user_profile', timeout=2.5)  # user_profie
solr_se = pysolr.SolrCloud(zk_md4, 'search_event', timeout=0.5)  # search_event
solr_np = pysolr.SolrCloud(zk_md4, 'news_profile', timeout=0.1)  # news_profile
solr_tp = pysolr.SolrCloud(zk_md4, 'topic_profile', timeout=0.1)  # topic_profile
solr_tt = pysolr.SolrCloud(zk_md7, 'topic_tpl', timeout=1.0)  #############
solr_topics = pysolr.SolrCloud(zk_md7, 'topics', timeout=1.0)
solr_news = pysolr.SolrCloud(zk_md7, 'robot_news', timeout=1.0)

solr_nat = pysolr.SolrCloud(zk_md7, 'news_and_topic', timeout=1.0)
solr_sme = pysolr.SolrCloud(zk_md4, 'simple_medical_entity', timeout=0.5)

solr_user_topn_topics = pysolr.SolrCloud(zk_md7, 'user_topn_topics', timeout=0.1)
solr_user_topn_news = pysolr.SolrCloud(zk_md7, 'user_topn_news', timeout=0.1)

solr_full_problem = pysolr.SolrCloud(zk_md7, 'full_problem', timeout=0.5)


class SolrQuery(object):
    def __init__(self):
        self.vals = {}

    def set(self, key, value):
        self.vals[key] = value

    def add(self, key, value):
        if key not in self.vals:
            self.vals[key] = []
        self.vals[key].append(value)

    def get_query_dict(self):
        return self.vals


def get_some_uid():
    # 选出一些user_id
    solr_query = SolrQuery()
    query = '*:*'
    solr_query.set('q', query)
    solr_query.set('fl', ['id'])  # 1506863248000
    solr_query.add('fq', 'last_ask:[1506863248000 TO *]')
    solr_query.set('rows', 500000)
    uids = set()
    for item in solr_up.search(**solr_query.get_query_dict()):
        # print item
        uids.add(item['id'])
    return uids


def parse_fq_plus(fq_plus, solr_query):
    if isinstance(fq_plus, basestring):
        solr_query.add('fq', fq_plus)
    elif isinstance(fq_plus, list):
        for i in fq_plus:
            solr_query.add('fq', i)
    elif isinstance(fq_plus, dict):
        for key in fq_plus:
            solr_query.add('fq', '%s:%s' % (key, fq_plus[key]))


def get_cy_event_row_key(uid, solr, fq_plus=None):
    # 从search_event里选出某用户的搜索行为的HBASE里的key
    # row_keys用于cy_event表
    # 这个老慢了，不要了
    solr_query = SolrQuery()
    query = '*:*'
    solr_query.set('q', query)
    solr_query.set('fl', ['id'])
    solr_query.set('rows', 100)
    solr_query.add('fq', 'uid:%s' % uid)
    if fq_plus:
        parse_fq_plus(fq_plus, solr_query)

    row_keys = []
    for item in solr.search(**solr_query.get_query_dict()):
        row_keys.append(item['id'])
    return row_keys


def get_row_key_from_solr(uid, col_name, fq_plus=None):
    if col_name == 'search_event':
        return get_cy_event_row_key(uid, solr_se, fq_plus)
    if col_name == 'news_profile':
        return get_cy_event_row_key(uid, solr_np, fq_plus)
    if col_name == 'topic_profile':
        return get_cy_event_row_key(uid, solr_tp, fq_plus)


def get_cy_event_row_key_search(uid, num=50):
    return get_cy_event_row_key2(uid, solr_se, num=num)


def get_cy_event_row_key_news(uid, num=50):
    return get_cy_event_row_key2(uid, solr_np, num=num)


def get_cy_event_row_key_topic(uid, num=50):
    return get_cy_event_row_key2(uid, solr_tp, num=num)


def get_row_key_from_solr2(uid, begin, end, col_name):
    if col_name == 'search_event':
        res = get_cy_event_row_key_search(uid)
    if col_name == 'news_profile':
        res = get_cy_event_row_key_news(uid)
    if col_name == 'topic_profile':
        res = get_cy_event_row_key_topic(uid)

    selected_rowkey_list = []
    begin = ensure_second_timestamp(begin)
    end = ensure_second_timestamp(end)
    for item in res:
        ts = ensure_second_timestamp(item['event_time'])
        if ts > end or ts < begin:
            continue
        selected_rowkey_list.append(item['id'])
    return selected_rowkey_list


def get_cy_event_row_key2(uid, solr, num=50):
    # 从search_event里选出某用户的搜索行为的HBASE里的key
    # row_keys用于cy_event表
    # 容易超时
    bad_return = []
    solr_query = SolrQuery()
    query = '*:*'
    solr_query.set('q', query)
    solr_query.set('fl', ['id', 'event_time'])
    solr_query.set('rows', num)
    solr_query.add('fq', 'uid:%s' % uid)
    solr_query.set('sort', 'event_time desc')

    max_try = 2
    cnt = 0
    while cnt < max_try:
        try:
            res = [item for item in solr.search(**solr_query.get_query_dict())]
            return res
        except Exception, e:
            print "get_cy_event_row_key2 exception", e
        finally:
            cnt += 1
    return bad_return


def get_user_last_bs_row_key(uid):
    # 从search_event里选出某用户的搜索行为的HBASE里的key
    # row_keys用于cy_event表

    solr_query = SolrQuery()
    query = '*:*'
    solr_query.set('q', query)
    solr_query.set('fl', ['id'])
    solr_query.set('rows', 1)
    solr_query.add('fq', 'uid:%s' % uid)
    solr_query.set('sort', 'event_time desc')
    return [item['id'] for item in solr_se.search(**solr_query.get_query_dict())]


def get_cy_event_row_key_with_ts(uid, col_name, fq_plus=None):
    if col_name == "search_event":
        solr = solr_se
    elif col_name == "news_profile":
        solr = solr_np

    solr_query = SolrQuery()
    query = 'uid:%s' % uid
    solr_query.set('q', query)
    solr_query.set('fl', ['id', 'event_time'])
    solr_query.set('rows', 500000)
    if fq_plus:
        parse_fq_plus(fq_plus, solr_query)

    row_keys = []
    ts = {}
    for item in solr.search(**solr_query.get_query_dict()):
        row_keys.append(item['id'])
        ts[item['id']] = item['event_time']
    return row_keys, ts


def vec2string(vec):
    try:
        vec = vec.tolist()
    except:
        pass

    return ','.join(['{:.5f}'.format(num) for num in vec])


def get_candidate_news(tag_center_string, content_type, rows=100, fq_plus=None, fl_plus=None):
    # 余弦相似度最接近的rows个
    solr_query = SolrQuery()

    solr_query.set('start', 0)
    solr_query.set('rows', 10000)  # 设置一个很大的数字
    # solr_query.set('rows',200)
    solr_query.set('cust_rows', rows)
    query = u'*:*'

    solr_query.set('q', query)
    solr_query.set('fl', ['id', 'score'] + fl_plus if fl_plus else [])
    solr_query.set('query_vec', tag_center_string)

    solr_query.add('fq', 'content_type:%s' % content_type)
    if fq_plus:
        parse_fq_plus(fq_plus, solr_query)

    res = [i for i in solr_tt.search(**solr_query.get_query_dict())]

    # end_time = datetime.datetime.now()
    # total_seconds = '%0.3f' % (end_time - start_time).total_seconds()
    # logging.info('%s_execute_seconds_%s query_dict: %s' % ('get_candidate_tt_from_slor', total_seconds,
    #                                                        solr_query.get_query_dict()))

    return res


def get_candidate_news2(tag_center_string, content_type, rows=100, cons=None, limits=None):
    # 按规则去掉不推的文章
    fq_plus = {"tid": 1}
    if cons:
        for con in cons:  #####这里有问题
            fq_plus["-clinic_no"] = con

    return get_candidate_news(tag_center_string, content_type, rows=rows, fq_plus=fq_plus, fl_plus=["clinic_no"])


def generate_tags_search_query(tags, limits):
    query = " OR ".join(["content:*%s*" % x for x in tags])
    if limits is not None:  # limits is a list
        query = "(" + query + ") AND (" + " OR ".join(["clinic_no:%s" % x for x in limits]) + ")"
    return query


def search_by_tags(tags, content_type, rows=50, cons=None, limits=None):
    solr_query = SolrQuery()
    query = generate_tags_search_query(tags, limits)
    print "search_by_tags query", query
    solr_query.set('q', query)
    solr_query.set('fl', ['id', 'score'])
    solr_query.set('rows', rows)
    fq = {"content_type": content_type, "tid": 1}
    if cons:
        for con in cons:  #####这里有问题
            fq["-clinic_no"] = con
    parse_fq_plus(fq, solr_query)
    res = [i for i in solr_tt.search(**solr_query.get_query_dict())]
    if len(res) == 0:
        return []
    best_score = res[0]['score']
    return [item['id'] for item in res if item['score'] == best_score]


def get_news_tags_from_solr(id):
    solr_query = SolrQuery()
    query = "id:%s" % id
    solr_query.set('q', query)
    solr_query.set('fl', ['content'])
    tags = []
    for item in solr_tt.search(**solr_query.get_query_dict()):
        if 'content' not in item:
            continue
        tags.extend(item['content'].split('|||'))
    return tags


def more_news_from_solr(text, tags, weights, rows,
                        news_type_cons=None, news_type_limits=None,
                        fq_plus=None):
    solr_query = SolrQuery()
    query_list0 = ["(title:%s)^1.2" % text]
    weights1 = [x * 6.0 for x in weights]
    query_list1 = ["(title_tag:%s)^%s" % (tags[i], weights1[i]) for i in range(len(tags))]
    weights2 = [x for x in weights]
    query_list2 = ["(news_tag:%s)^%s" % (tags[i], weights2[i]) for i in range(len(tags))]

    query = " OR ".join(query_list0 + query_list1 + query_list2)

    solr_query.set('q', query)
    solr_query.set('rows', rows)
    solr_query.set('fl', ['id', 'title'])
    print "more_news_from_solr query", query

    if fq_plus:
        parse_fq_plus(fq_plus, solr_query)
    if news_type_cons:
        for con in news_type_cons:
            solr_query.add('fq', '-news_type:%s' % con)
    if news_type_limits:
        for limit in news_type_limits:
            solr_query.add('fq', 'news_type:%s' % limit)
    print "solr_query.get_query_dict()", solr_query.get_query_dict()
    res_ids = []
    title_dict = {}
    # score_dict = {}
    for item in solr_news.search(**solr_query.get_query_dict()):
        # id = item['id']
        try:
            id = int(item['id'])
        except:
            continue
        title = item['title']

        res_ids.append(id)
        title_dict[id] = title
        # score_dict[id] = score
    return res_ids, title_dict


def more_topic_from_solr(text, tags, weights, rows):
    # # 从md7 topics表里召回topic（大搜也是这么做的，但是不返回score),加入了weights
    solr_query = SolrQuery()
    query_list0 = ["(title:%s)^1.2" % text] if text else []
    weights1 = [2.0 * x for x in weights]
    query_list1 = ["(title:%s)^%s" % (tags[i], weights1[i]) for i in range(len(tags))]
    query = " OR ".join(query_list0 + query_list1)
    solr_query.set('q', query)
    solr_query.set('fl', ['id', 'title', 'score'])
    solr_query.set('rows', rows)

    res_ids = []
    title_dict = {}
    # score_dict = {}
    for item in solr_topics.search(**solr_query.get_query_dict()):
        try:
            id = int(item['id'])
        except:
            continue
        title = item['title']
        if len(title) == 0:
            continue
        score = item['score']
        res_ids.append(id)
        title_dict[id] = title
        # score_dict[id] = score

    return res_ids, title_dict  # , score_dict


def get_max_true_id(material_type):
    material_type = str(material_type)
    solr_query = SolrQuery()
    q = '*:*'
    solr_query.add('fq', 'type:%s' % material_type)
    solr_query.set('q', q)
    solr_query.set('rows', 1)
    solr_query.set('sort', 'true_id desc')
    solr_query.set('fl', ['id'])
    for item in solr_nat.search(**solr_query.get_query_dict()):
        mtype, id = item['id'].split('_')
        if str(mtype) == material_type:
            return int(id)


def more_news_and_topic_from_solr(text, tags, weights, rows, drug_words=None, news_cons=None, news_limits=None,
                                  topic_only=False):
    # very  important function
    bad_return = [], {}, {}
    if len(weights) == 0:
        return bad_return
    if not drug_words:
        drug_words = []
    # md4 news_and_topic
    print "drug_words", '-'.join(drug_words)
    solr_query = SolrQuery()

    text_query_list0 = ["(query_text:%s)^1.2" % text] if text else []
    weights1 = [x * 2.0 for x in weights]

    mean_factor = sum([1.2] + weights1) / (len(weights1) + 1) if text else sum(weights1) / len(weights1)

    text_query_list1 = ["(query_text:%s)^%s" % (tags[i], weights1[i]) for i in range(len(tags)) if not
    tags[i] in drug_words]
    text_query = " OR ".join(text_query_list0 + text_query_list1)

    if drug_words:
        drug_query = " OR ".join(['query_text:"%s"' % drug for drug in drug_words])
        text_query = '(%s) AND (%s)' % (drug_query, text_query)
    print "text_query", text_query

    if topic_only:
        solr_query.add('fq', "type:topic AND -topic_score:0")
    elif news_cons or news_limits:
        if news_cons:
            a = "(" + ' OR '.join(news_cons) + ')'
            solr_query.add('fq', '(type:news AND -news_type:%s) OR (type:topic AND -topic_score:0)' % a)
        if news_limits:
            a = "(" + ' OR '.join(news_limits) + ')'
            solr_query.add('fq', '(type:news AND news_type:%s) OR (type:topic AND -topic_score:0)' % a)

    #去掉标题is_horrible=1的文章
    solr_query.add('fq','is_horrible:0')

    solr_query.set('q', text_query)
    solr_query.set('rows', rows)
    solr_query.set('fl', ['id', 'score', 'title'])

    res_ids = []
    title_dict = {}
    score_dict = {}

    cnt = 0
    max_score = None
    for item in solr_nat.search(**solr_query.get_query_dict()):
        id = item['id']
        score = item['score']
        # print "solr", id, score

        if cnt == 0:
            max_score = score
            if max_score <= 10.0 * mean_factor:
                print "%s < %s" % (max_score, 10.0 * mean_factor)
                return bad_return

        score /= max_score
        res_ids.append(id)
        title_dict[id] = item['title']
        score_dict[id] = score
        if cnt == 0:
            cnt += 1
    return res_ids, title_dict, score_dict


def cut_weight(query_weight_dict, cut_bottom=0.01):
    for key in query_weight_dict:
        if query_weight_dict[key] <= cut_bottom:
            query_weight_dict[key] = cut_bottom


def more_news_from_solr_nat(text_tf_list, tags, weights, rows, news_cons=None, news_limits=None):
    # more_news_and_topic_from_solr only news version
    # for feed news
    bad_return = [], {}, {}

    if len(weights) == 0:
        return bad_return

    solr_query = SolrQuery()

    # 去掉text中的数字
    # text = re.sub(u'[0-9男女岁]{1,5}', '', text)
    # text = remove_other(text, u' ')

    # text_tf_list = [[remove_other(re.sub(u'[0-9男女岁]{1,5}', '', item[0])), item[1]] for item in text_tf_list]
    text_tf_list = []
    tag_weight_list = [[tags[i], weights[i]] for i in range(len(tags))]
    text_weight_dict = dict(text_tf_list)
    tag_weight_dict = dict(tag_weight_list)
    query_weight_dict = text_weight_dict
    query_weight_dict.update(tag_weight_dict)  # 顺序在text_weight_dict之后
    cut_weight(query_weight_dict)
    query_list = ["(query_text:%s)^%s" % (key, query_weight_dict[key]) for key in query_weight_dict]

    # text_query_list0 = ["(query_text:%s)^%s" % (item[0], item[1]) for item in text_tf_list]
    # weights1 = [x * 2.0 for x in weights]

    # mean_factor = sum([1.2] + weights1) / (len(weights1) + 1) if text else sum(weights1) / len(weights1)

    # text_query_list1 = ["(query_text:%s)^%s" % (tags[i], weights1[i]) for i in range(len(tags)) if not
    # tags[i] in drug_words]
    text_query = " OR ".join(query_list)
    print "text_query", text_query

    if news_cons or news_limits:
        if news_cons:
            a = "(" + ' OR '.join(news_cons) + ')'
            solr_query.add('fq', 'type:news AND -news_type:%s' % a)
        if news_limits:
            a = "(" + ' OR '.join(news_limits) + ')'
            solr_query.add('fq', 'type:news AND news_type:%s' % a)

    ########限制id以限制文章时间
    solr_query.add('fq', 'true_id:[60551 TO *]')
    # 去掉标题is_horrible=1的文章
    solr_query.add('fq', 'is_horrible:0')

    solr_query.set('q', text_query)
    solr_query.set('rows', rows)
    solr_query.set('fl', ['id', 'score', 'title'])

    res_ids = []
    title_dict = {}
    score_dict = {}

    cnt = 0
    max_score = None
    BAD_IDS = ['news_53013', 'news_55466', 'news_12229', 'news_57629',
               'news_9084', 'news_57524', 'news_52806', 'news_9084',
               'news_55268', 'news_124']
    for item in solr_nat.search(**solr_query.get_query_dict()):
        id = item['id']
        if id in BAD_IDS:
            continue
        score = item['score']
        # print "solr", id, score

        if cnt == 0:
            max_score = score
        # if max_score <= 5.0 * mean_factor:
        #         print "%s < %s" % (max_score, 10.0 * mean_factor)
        #         return bad_return

        score /= max_score
        res_ids.append(id)
        title_dict[id] = item['title']
        score_dict[id] = score
        if cnt == 0:
            cnt += 1
    return res_ids, title_dict, score_dict


def more_topic_from_solr_old(query, rows=10):
    # 从md7 topics表里召回topic（大搜也是这么做的，但是不返回score)
    # 不用了
    solr_query = SolrQuery()
    query = "title:%s" % query
    solr_query.set('q', query)
    solr_query.set('fl', ['id', 'title', 'score'])
    solr_query.set('rows', 2 * rows)
    # solr_query.add('fq',["-title:''"])solr里存的都是有标题的
    res0 = [item for item in solr_topics.search(**solr_query.get_query_dict())]
    if len(res0) == 0:
        return {}
    best_score = res0[0]['score']
    threshhold = 0.5 * best_score  # 取最高分的一半作为召回的阈值
    return dict([(item['id'], item['title']) for item in res0 if item['score'] > threshhold])


def user_view_news_time(uid, newsid, fq_plus=None):
    # news_profile表里搜最后浏览的时间戳
    solr_query = SolrQuery()
    query = 'uid:%s' % uid
    solr_query.set('q', query)
    solr_query.set('fl', ['event_time'])
    solr_query.set('rows', 1)
    solr_query.set("sort", "event_time desc")  # 按时间倒排
    if fq_plus:
        parse_fq_plus(fq_plus, solr_query)
    solr_query.add('fq', 'news_id:%s' % newsid)

    times = []
    for item in solr_np.search(**solr_query.get_query_dict()):
        times.append(item['event_time'])
    return times


def get_bodypart_word(text):
    # md4 simple_medical_entity
    text = ensure_unicode(text)
    solr_query = SolrQuery()
    query = 'name:%s' % text
    solr_query.set('q', query)
    solr_query.set('fl', ['name'])
    solr_query.set('rows', 3)
    solr_query.set('fq', 'type:bodypart')
    bp_words = []
    for item in solr_sme.search(**solr_query.get_query_dict()):
        names = item['name']
        for name in names:
            if name in text:
                bp_words.append(name)
    return bp_words


def nat_get_title(id):
    bad_return = ""
    query = "*:*"
    solr_query = SolrQuery()
    solr_query.set('q', query)
    solr_query.set('fl', ['title'])
    solr_query.set('rows', 1)
    solr_query.set('fq', 'id:%s' % id)
    title = bad_return
    for item in solr_nat.search(**solr_query.get_query_dict()):
        title = item['title']
    return title


def nat_get_newstype(id):
    bad_return = ""
    query = "*:*"
    solr_query = SolrQuery()
    solr_query.set('q', query)
    solr_query.set('fl', ['news_type'])
    solr_query.set('rows', 1)
    solr_query.set('fq', 'id:%s' % id)
    news_type = bad_return
    for item in solr_nat.search(**solr_query.get_query_dict()):
        news_type = item.get('news_type', bad_return)
    return news_type


def nat_get_digest(id):
    bad_return = ""
    query = "*:*"
    solr_query = SolrQuery()
    solr_query.set('q', query)
    solr_query.set('fl', ['digest'])
    solr_query.set('rows', 1)
    solr_query.set('fq', 'id:%s' % id)
    digest = bad_return
    for item in solr_nat.search(**solr_query.get_query_dict()):
        digest = item.get('digest', bad_return)
    return digest


def nat_get_topic_score(id):
    bad_return = 0
    query = "*:*"
    solr_query = SolrQuery()
    solr_query.set('q', query)
    solr_query.set('fl', ['topic_score'])
    solr_query.set('rows', 1)
    solr_query.set('fq', 'id:%s' % id)
    topic_score = bad_return
    for item in solr_nat.search(**solr_query.get_query_dict()):
        topic_score = item.get('topic_score', bad_return)
    return topic_score / 10.0


def get_entity_cate(word):
    # md4 simple_medical_entity表 获取word对应的实体词类型
    # 非常慢，不在用
    bad_return = ''
    word = word.lower()
    query = "name_string:%s" % word
    solr_query = SolrQuery()
    solr_query.set('q', query)
    solr_query.set('fl', ['type'])
    solr_query.set('rows', 1)

    res = [item['type'] for item in solr_sme.search(**solr_query.get_query_dict())]
    if not res:
        return bad_return
    return res[0]


def get_caled_user_topn_topics(key):
    bad_return = None  # None是没算过，不在solr里的，[]是算过但是没有的
    solr_query = SolrQuery()
    query = '*:*'
    solr_query.set('q', query)
    solr_query.add('fq', 'id:%s' % key)
    solr_query.set('fl', ['topic_ids'])
    solr_query.set('rows', 1)
    res = [item['topic_ids'] for item in solr_user_topn_topics.search(**solr_query.get_query_dict())]
    if res:
        return json.loads(res[0])
    return bad_return


def get_caled_user_topn_topics_yesterday(uid):
    key = "%s|%s" % (get_yesterday_date(), uid)
    return get_caled_user_topn_topics(key)


def get_caled_user_topn_news(uid):
    bad_return = []
    solr_query = SolrQuery()
    query = '*:*'
    solr_query.set('q', query)
    solr_query.add('fq', 'id:%s' % uid)
    solr_query.set('fl', ['news_ids'])
    solr_query.set('rows', 1)
    res = [item['news_ids'] for item in solr_user_topn_news.search(**solr_query.get_query_dict())]

    if res:
        return json.loads(res[0])
    return bad_return


def get_user_search_keys(uid, begin, end):
    # 从md4的search_event中选取一定时间段内用户搜索行为的key
    # （这个key在hbase的cy_event表中可以查到该次行为的详细信息）

    # 调整两个时间戳的格式
    begin = int(1000 * ensure_second_timestamp(begin))
    end = int(1000 * ensure_second_timestamp(end))

    # 建立query
    solr_query = SolrQuery()
    q = '*:*'
    solr_query.set('q', q)
    solr_query.set('fl', ['id', 'event_time'])
    # solr_query.add('fq', 'event_time:[%s TO %s]' % (begin, end))
    solr_query.add('fq', 'uid:%s' % uid)
    solr_query.set('rows', 100000)

    # 搜
    res = [[item['id'], item['event_time']] for item in solr_se.search(**solr_query.get_query_dict())]
    res = [item[0] for item in res if (begin < item[1] < end)]
    return res


def get_user_search_keys_smart(uid, num=5, mode='search'):
    # begin = int(1000 * ensure_second_timestamp(begin))
    # end = int(1000 * ensure_second_timestamp(end))

    # 建立query
    solr_query = SolrQuery()
    q = '*:*'
    solr_query.set('q', q)
    solr_query.set('fl', ['id', 'event_time'])
    # solr_query.add('fq', 'event_time:[%s TO %s]' % (begin, end))
    solr_query.add('fq', 'uid:%s' % uid)
    solr_query.set('rows', num)
    solr_query.set('sort', 'event_time desc')

    if mode == 'search':
        solr = solr_se
    elif mode == 'news':
        solr = solr_np
    elif mode == 'topic':
        solr = solr_tp
    else:
        return []

    # 搜
    try:
        res = [item['id'] for item in solr.search(**solr_query.get_query_dict())]
    except:
        res = []

    return res[:num]


def get_last_login_uids(begin, end):
    # 不需要快
    # 调整两个时间戳的格式
    begin = int(1000 * ensure_second_timestamp(begin))
    end = int(1000 * ensure_second_timestamp(end))

    # 建立query
    solr_query = SolrQuery()
    q = '*:*'
    solr_query.set('q', q)
    solr_query.set('fl', ['id'])
    solr_query.add('fq', 'last_login:[%s TO %s]' % (begin, end))
    solr_query.set('rows', 1000000)

    # 搜
    res = [item['id'] for item in solr_up.search(**solr_query.get_query_dict())]
    return res


def user_last_pids(uid, num, key_prefix='qa_'):
    # 热卖推荐用，获取用户最后的pid，然而不包括当天的，因为solr里没有，需要今天的应该去habse的cy_real_time_event整
    # 不通用
    solr_query = SolrQuery()
    q = '*:*'
    solr_query.set('q', q)
    solr_query.set('fl', ['id', 'create_time'])
    solr_query.add('fq', 'uid:%s' % uid)
    solr_query.set('rows', num)
    solr_query.set('sort', 'create_time desc')
    res = [[key_prefix + str(item['id']), int(item['create_time'])] for item in
           solr_full_problem.search(**solr_query.get_query_dict())]
    return res


def test():
    import time
    t1 = time.time()
    a = user_last_pids(86679618, 2)
    t2 = time.time()
    print 'time', t2 - t1
    print 'a', a


if __name__ == '__main__':
    test()
