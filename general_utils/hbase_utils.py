# encoding=utf8

import json
import time
import random
from collections import defaultdict
import happybase
from general_utils.time_utils import get_48h_timestamps, half_year_before_timestamp, get_today_timestamp
from medweb_utils.utilities.log_utils import info_logger
from general_utils.solr_utils import get_row_key_from_solr, get_cy_event_row_key_with_ts, get_row_key_from_solr2, \
    get_user_last_bs_row_key, get_user_search_keys, get_user_search_keys_smart
from general_utils.time_utils import ensure_second_timestamp, ensure_m_timestamp, get_ndays_before_timestamp, \
    timestamp2datetime

CY_REAL_TIME_EVENT_ATTR_MAP = {
    # cy_real_time_event中事件类型与需要取的数据的对应关系，估计以后还会改
    "big_search": "info:query",
    "view_news": "info:news_id",
    "free_problem_create": "info:ask",
    "view_topic": "info:topic_id",
    "view_topics": "info:topic_id",
    "search_doctor": "info:query",
}

CY_REAL_TIME_EVENT_ATTR_MAP_BAD = {
    #
    "view_news": "info:topic_id",
    "view_topic": "info:news_id"
}

SMALL_TIMEOUT = 30000
BIG_TIMEOUT = 30000

def event_info(data, event_type):
    if "info:news_id" in data:
        return "view_news", data["info:news_id"]
    if "info:topic_id" in data:
        return "view_topic", data["info:topic_id"]
    if "info:ask" in data:
        return "free_problem_create", data["info:ask"] + ' ' + data["info:sex"] + ' ' + data["info:age"]
    return event_type, data[CY_REAL_TIME_EVENT_ATTR_MAP[event_type]]


def event_info2(data, event_type):
    # 只取big_search和free_problem_create
    if "info:ask" in data:
        return "free_problem_create", [data["info:ask"], data["info:sex"], data["info:age"]]
    return event_type, [data[CY_REAL_TIME_EVENT_ATTR_MAP[event_type]]]


def get_cy_event_active_user(begin, end):
    # 从cy_event中一段时间内有记录的用户id（不包括qa，这个表里没有）
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=BIG_TIMEOUT)
    table = connection.table("cy_event")
    begin = ensure_m_timestamp(begin)
    end = ensure_m_timestamp(end)

    uids = set()
    for key, value in table.scan(batch_size=10000):

        try:
            action_type, ts, uid = key.split('|')
        except:
            continue
        ts = int(ts)
        if ts > end or ts < begin:
            continue
        if random.random() > 0.01:
            continue
        uids.add(uid)

    connection.close()
    return uids


def get_today_data(now=None):
    # 时间戳格式是hbase除以1000之后的
    # 获取now当天的活跃用户数据

    if now:
        # 从指定的now时间戳算前两天（不包括now的当天)
        now = ensure_second_timestamp(now)
        begin, end = get_today_timestamp(now)
    else:
        begin, end = get_today_timestamp()
    return cy_time_event_kernel(begin, end)


def get_test_data(begin):
    end = begin + 10000.0  # one hour
    return cy_time_event_kernel(begin, end)


def get_48h_data(now=None):
    # 时间戳格式是hbase除以1000之后的
    if now:
        # 从指定的now时间戳算前两天（不包括now的当天)
        now = ensure_second_timestamp(now)
        begin, end = get_48h_timestamps(now)
    else:
        begin, end = get_48h_timestamps()
    return cy_time_event_kernel(begin, end)


def get_sp_duration_valid_user_id(begin, end):
    '''
    获取begin,end之间所有活跃(big_search and free_problem_create)用户的id
    :param begin:
    :param end:
    :return:
    '''

    # 调整输入时间戳格式
    begin = ensure_second_timestamp(begin)
    end = ensure_second_timestamp(end)

    # 获取table
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=BIG_TIMEOUT)
    table = connection.table("cy_real_time_event")

    valid_uids = set()
    event_type_list = ["big_search", "free_problem_create"]
    for key, data in table.scan():
        uid, timestamp, event_type = key.split('|')
        if event_type not in event_type_list:
            continue
        ts = ensure_second_timestamp(timestamp)
        if ts < begin or ts > end:
            continue
        valid_uids.add(int(uid))

    connection.close()
    return valid_uids


def get_sp_duration_active_userid(begin, end):
    '''
    获取cy_real_time_event一定时间段内左右出现的用户id
    :param begin:
    :param end:
    :return:
    '''

    # 调整输入时间戳格式
    begin = ensure_second_timestamp(begin)
    end = ensure_second_timestamp(end)
    # 获取table
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=BIG_TIMEOUT)
    table = connection.table("cy_real_time_event")

    uids = set()
    for key, data in table.scan():
        uid, timestamp, event_type = key.split('|')
        try:
            uid = int(uid)
        except:
            continue
        ts = ensure_second_timestamp(timestamp)
        if ts < begin or ts > end:
            continue

        uids.add(uid)
    connection.close()
    return list(uids)


def get_sp_duration_valid_user_data(begin, end, test_uid=None):
    '''
    获取begin到end之间所有活跃用户（qa or bs action）的用户的数据
    时间不可以太久，因为cy_real_time_event只存10天的实时数据
    :param begin: 开始的时间戳
    :param end: 终止的时间戳
    :return: user_info0
    '''
    # 调整输入时间戳格式
    begin = ensure_second_timestamp(begin)
    end = ensure_second_timestamp(end)

    # 获取table
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=BIG_TIMEOUT)
    table = connection.table("cy_real_time_event")

    user_info0 = {}
    event_type_list = ["big_search", "free_problem_create"]

    row_prefix = str(test_uid) if test_uid else None

    for key, data in table.scan(row_prefix=row_prefix):
        uid, timestamp, event_type = key.split('|')
        uid = int(uid)
        if test_uid and uid != int(test_uid):
            continue
        if event_type not in event_type_list:
            continue
        ts = ensure_second_timestamp(timestamp)
        if ts < begin or ts > end:
            continue

        # 搂uid的数据，不记录last_event
        if uid not in user_info0:
            user_info0[uid] = {'big_search': [], 'free_problem_create': []}

        event_type, t_info = event_info2(data, event_type)
        user_info0[uid][event_type].append(t_info + [timestamp])

    connection.close()
    return user_info0


def cy_time_event_kernel_test(begin, end, test_uid=None):
    ############
    # query算15min内的所有，qa取最后一个，所以一个用户就取一次触发的例子，其他时间不要了
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=BIG_TIMEOUT)
    table = connection.table("cy_real_time_event")

    data_dict = {}

    interval = 5 * 60.0  # 15min
    caled_uid = set()
    max = 0.0

    begin = ensure_second_timestamp(begin)
    end = ensure_second_timestamp(end)
    row_prefix = str(test_uid) + '|' if test_uid else None
    for key, data in table.scan(row_prefix=row_prefix):
        uid, timestamp, event_type = key.split('|')
        if test_uid and int(uid) != int(test_uid):
            continue
        if event_type not in ("big_search", "free_problem_create"):
            continue

        timestamp = ensure_second_timestamp(timestamp)
        if timestamp > max:
            max = timestamp
        if timestamp > end or timestamp < begin:
            continue

        if uid in caled_uid:
            continue
        caled_uid.add(uid)

        end_t = timestamp + 1.0
        begin_t = end_t - interval
        user_info = cy_time_event_one_user_kernel(uid, begin_t, end_t)
        data_dict[uid] = user_info
    print "num of caled_uid", len(caled_uid)
    print "max timestamp", max
    return data_dict


def cy_time_event_one_user_viewnews(uid, begin, end):
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("cy_real_time_event")
    res = {}
    for key, data in table.scan(row_prefix=str(uid) + '|'):
        uid, timestamp, event_type = key.split('|')

        if event_type not in ["view_news"]:
            continue
        timestamp = ensure_second_timestamp(timestamp)
        info_logger.info("real timestamp=%s", timestamp)
        if timestamp >= begin and timestamp <= end:
            news_id = int(data["info:news_id"])
            res[news_id] = timestamp
    return res


def cy_time_event_one_user_kernel(uid, begin, end, event_type_list=None):
    # 获取某个用户begin到end时间戳内的所有活动信息
    # 上线用的，输入uid，时间段（一般是15min，获取触发类型和触发信息）

    info = {"last_event": None,
            "last_event_time": 0}

    if not event_type_list:
        event_type_list = ["big_search", "free_problem_create"]

    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("cy_real_time_event")
    for key, data in table.scan(row_prefix=str(uid) + '|'):
        uid, timestamp, event_type = key.split('|')

        if event_type not in event_type_list:
            continue
        timestamp = ensure_second_timestamp(timestamp)
        if end >= timestamp >= begin:
            event_type, t_info = event_info2(data, event_type)
            if event_type in info:
                info[event_type].append(t_info + [timestamp])
            else:
                info[event_type] = [t_info + [timestamp]]
            if timestamp > info['last_event_time']:
                info['last_event_time'] = timestamp
                info['last_event'] = [event_type, t_info]

    connection.close()
    return info


def cy_time_event_one_user_kernel2(uid, begin, end):
    '''
    与cy_time_event_one_user_kernel输出数据格式不同而已
    :param uid:
    :param begin: ms ts
    :param end: ms ts
    :return:
    '''
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=BIG_TIMEOUT)
    table = connection.table("cy_real_time_event")
    action_list = []  # list of [ts(ms),action_obj,action_type]
    for key, data in table.scan(row_prefix=str(uid) + '|'):
        uid, timestamp, event_type = key.split('|')
        timestamp = int(timestamp)
        if timestamp < begin or timestamp > end:
            continue

        # qa event
        if 'free_problem' in event_type:
            action_type = 'qa'
            obj = unicode(data['info:ask'])
        elif 'big_search' in event_type:
            action_type = 'bs'
            obj = unicode(data['info:query'])
        elif 'search_doctor' in event_type:
            action_type = 'sd'
            obj = unicode(data['info:query'])
        elif 'view_news' in event_type:
            action_type = 'vn'
            obj = int(data['info:news_id'])
        elif 'view_topic' in event_type:
            action_type = 'vt'
            obj = int(data['info:topic_id'])
        else:
            continue
        action_list.append([timestamp, obj, action_type])

    connection.close()
    return action_list


def yesterday_user_info(uid, timestamp=None):
    begin = get_ndays_before_timestamp(1, timestamp)
    end = begin + 86400.0
    return cy_time_event_one_user_kernel(uid, begin, end)


def cy_time_event_kernel(begin, end):
    cnt = 0
    all_alive_users = set()

    event_count = defaultdict(int)
    data_dict = {}
    print "begin", begin
    print "end", end
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=BIG_TIMEOUT)
    table = connection.table("cy_real_time_event")
    max = 0.0
    for key, data in table.scan():
        uid, timestamp, event_type = key.split('|')
        timestamp = float(timestamp) / 1000.0
        if timestamp > max:
            max = timestamp
        if end >= timestamp >= begin:
            if uid not in data_dict:
                data_dict[uid] = {"key": []}

            real_event_type, info = event_info(data, event_type)
            if real_event_type in data_dict[uid] and info not in data_dict[uid][real_event_type]:

                data_dict[uid][real_event_type].append(info)
            else:
                data_dict[uid][real_event_type] = [info]
            data_dict[uid]["key"].append(key)

            # uid = data['info:uid']
            event_count[event_type] += 1
            all_alive_users.add(uid)
            cnt += 1

    print cnt
    print "max timestamp", max
    print len(all_alive_users)
    for event_type in event_count:
        print event_type, event_count[event_type]
    connection.close()
    return data_dict


def user_half_year_topics(uid):
    if uid is None:
        return set()

    t1 = time.time()
    row_key_list = get_row_key_from_solr2(uid=uid, begin=half_year_before_timestamp(), end=time.time(),
                                          col_name='topic_profile')
    t2 = time.time()
    topic_ids = get_news_id_from_cy_event(row_key_list, "info:topic_id")
    t3 = time.time()
    print "user_half_year_topics time", t2 - t1, t3 - t2
    return topic_ids  # a set


def user_half_year_newsids(uid):
    if uid is None:
        return set()
    row_key_list = get_row_key_from_solr2(uid=uid, begin=half_year_before_timestamp(), end=time.time(),
                                          col_name='news_profile')
    news_ids = get_news_id_from_cy_event(row_key_list, "info:news_id")
    return news_ids  # a set


def user_last_query(uid):
    # 优先从hbase的cy_real_time_event中取，若没有（用户10天没动作），则从solr search_event取最后一个key ,用这个key到hbase的cy_event中取
    event_type_dict = {"big_search": "info:query"}
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)

    table = connection.table("cy_real_time_event")
    last_timestamp = 0
    query = ''
    for key, data in table.scan(row_prefix=str(uid) + '|'):
        uid, timestamp, event_type = key.split('|')
        if event_type not in event_type_dict:
            continue
        if int(timestamp) > last_timestamp:
            last_timestamp = int(timestamp)
            query = data[event_type_dict[event_type]]
    connection.close()
    # 若cy_real_time_event里没有
    if not query:
        cy_event_key_list = get_user_last_bs_row_key(uid)
        if not cy_event_key_list:
            # 历史上该用户也没有搜索过
            return query
        q = get_news_id_from_cy_event(cy_event_key_list, "info:text")

        if q:
            return str(list(q)[0])  # hbase info:text字段有可能存错
    return query


def generate_cy_service_key(pids):
    return [str(x) + '||gf' for x in pids]


def get_news_id_from_cy_event(row_key_list, col='info:news_id'):
    # 从hbase的cy_event获取用户点击过的news_id
    cols = [col]
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("cy_event")
    row = table.rows(row_key_list, columns=cols)
    # row = get_cy_event_table().rows(row_key_list,
    #                                 columns=cols)

    news_id = set()
    for key, value in row:
        try:
            news_id.add(int(value[col]))
        except:
            news_id.add(value[col])
    connection.close()
    return news_id


def get_news_id_from_cy_event2(row_key_list, col='info:news_id'):
    # 从hbase的cy_event获取用户点击过的news_id
    # 热卖推荐取数据用，不能通用
    cols = [col]
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("cy_event")
    row = table.rows(row_key_list, columns=cols)
    # row = get_cy_event_table().rows(row_key_list,
    #                                 columns=cols)
    ts_dict = {}
    key_prefix = 'news_' if 'news' in col else 'topic_'
    for key, value in row:
        a, ts, b = key.split('|')
        ts = int(ts)
        id = value[col]
        key0 = key_prefix + str(id)
        ts_dict[key0] = ts
    connection.close()
    return ts_dict


def get_user_query(uid, begin, end):
    cols = ['info:text', 'info:search_type']
    col_limit = ['info:search_type', 'big_search']
    row_key_list = get_user_search_keys(uid, begin, end)

    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=BIG_TIMEOUT)
    table = connection.table("cy_event")
    row = table.rows(row_key_list, columns=cols)

    all_query = []
    for key, value in row:
        if value[col_limit[0]] != col_limit[1]:
            continue
        all_query.append(value[cols[0]].decode('utf8'))

    connection.close()

    return all_query


def get_user_query2(uid, num=5):
    # big_search and search_doctor are considered
    cols = ['info:text', 'info:search_type', 'info:filter']
    row_key_list = get_user_search_keys_smart(uid, num=num)
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("cy_event")
    row = table.rows(row_key_list, columns=cols)
    clinc_nos = []
    all_query = []

    for key, value in row:
        try:
            text = value['info:text'].decode('utf8')
            all_query.append(text)
        except:
            clinic_no_info = json.loads(value['info:filter'])
            print clinic_no_info
            if 'second_class_clinic_no' in clinic_no_info:
                clinc_nos.append(clinic_no_info['second_class_clinic_no'])
            elif 'clinic_no' in clinic_no_info:
                clinc_nos.append(clinic_no_info['clinic_no'])
    return all_query, clinc_nos


def get_user_query3(uid, num=5):
    cols = ['info:text', 'info:search_type', 'info:filter']
    row_key_list = get_user_search_keys_smart(uid, num=num)
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("cy_event")
    row = table.rows(row_key_list, columns=cols)
    text_dict = {}
    ts_dict = {}
    clinic_nos = []
    for key, value in row:
        action_type, ts, _ = key.split('|')
        ts = int(ts)

        try:
            text = value['info:text'].decode('utf8')
            text_dict[key] = text
            ts_dict[key] = ts
        except:
            clinic_no_info = json.loads(value['info:filter'])
            if 'second_class_clinic_no' in clinic_no_info:
                clinic_nos.append(clinic_no_info['second_class_clinic_no'])
            elif 'clinic_no' in clinic_no_info:
                clinic_nos.append(clinic_no_info['clinic_no'])
    return text_dict, ts_dict, clinic_nos


def get_all_realtimeevent_text():
    '''
    获取cy_real_time_event中所有数据中的文本.执行时间几分钟
    view_topics
    big_search
    view_news
    free_problem_create
    search_doctor
    :return:
    '''
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=BIG_TIMEOUT)
    table = connection.table("cy_real_time_event")
    texts = []
    for key, value in table.scan():
        try:
            uid, ts, action_type = key.split('|')
        except:
            continue
        if action_type == 'big_search':
            texts.append(unicode(value['info:query']))
        elif action_type == 'free_problem_create':
            texts.append(unicode(value['info:ask']))
    print 'texts num', len(texts)
    connection.close()
    return texts


def get_view_news_data(row_prefix):
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("cy_event")
    news_viwers = defaultdict(set)
    cnt = 0
    print time.time()
    last_ts = None
    now = time.time()
    start = now - 86400 * 180

    focused_type = ('view_news', 'view_topic')
    if row_prefix not in focused_type:
        return
    all_types = defaultdict(int)
    for key, data in table.scan(row_prefix=row_prefix):

        try:
            action_type, ts, uid = key.split('|')
        except:
            continue
        all_types[action_type] += 1

        if action_type not in focused_type:
            continue

        last_ts = ensure_second_timestamp(ts)
        if last_ts < start:
            continue
        news_id = data[CY_REAL_TIME_EVENT_ATTR_MAP[action_type]]
        news_viwers[news_id].add(uid)
        cnt += 1
        if cnt % 1000 == 0:
            print timestamp2datetime(time.time()), cnt, len(news_viwers)
    print time.time()
    print 'last_ts', last_ts

    print len(news_viwers)
    for x in all_types:
        print x, all_types[x]
    with open('cy_event_%s.json' % row_prefix, 'w') as f:
        for news_id in news_viwers:
            str = json.dumps({
                'id': news_id,
                'uids': list(news_viwers[news_id]),
                'len': len(news_viwers[news_id])
            }) + '\n'
            f.write(str)


def get_user_recent_views(uid, now=None, lookback=3 * 24 * 86400.0, num=None):
    # 获取用户近期点击news和topic的数据
    if now:
        end = ensure_second_timestamp(now)
        begin = end - lookback
    else:
        end = time.time()
        begin = end - lookback
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("cy_real_time_event")
    focused_type = ('view_news', 'view_topics', 'view_topic')
    actions = []

    o = [[key, data] for key, data in table.scan(row_prefix=str(uid) + '|')][::-1]
    for key, data in o:
        if num and len(actions) >= num:
            # 限定只取一定数目的数据
            break
        _, ts, action_type = key.split('|')
        # 用时间筛选
        ts = ensure_second_timestamp(ts)
        if end < ts < begin:
            continue
        if action_type in focused_type:
            actions.append([ts, action_type, int(data[CY_REAL_TIME_EVENT_ATTR_MAP[action_type]])])
    return actions


def get_qa_texts_by_pid(pid):
    # 很快~
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("problem2")
    t1 = time.time()
    o = table.rows([str(pid)], columns=['profile:user_asks'])
    t2 = time.time()
    print "get_qa_texts_by_pid hbase time", t2 - t1
    connection.close()
    try:
        data = o[0][1]
        # print data
        return data.get('profile:user_asks', '').decode('utf8', 'ignore').split('|||')
    except Exception, e:
        print "get_qa_texts_by_pid exception", e
        return []


def get_qa_texts_by_pids(pid_list, key_prefix='qa_'):
    pid_list = [str(pid) for pid in pid_list]
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=SMALL_TIMEOUT)
    table = connection.table("problem2")
    t1 = time.time()
    o = table.rows(pid_list, columns=['profile:user_asks', 'detail:ask'])
    t2 = time.time()
    print "get_qa_texts_by_pids hbase time", t2 - t1
    connection.close()
    text_dict = {}
    for key, data in o:
        key0 = key_prefix + key
        text = data.get('profile:user_asks', '').decode('utf8', 'ignore').replace('|||', ' ')
        if not text:
            text = data.get('detail:ask', '').decode('utf8', 'ignore')
        text_dict[key0] = text
    return text_dict


def test():
    import sys
    uid = sys.argv[1]
    q = user_last_query(uid)
    print q, type(q)


def test1():
    import sys
    import time
    uid = sys.argv[1]
    t1 = time.time()
    news_ids = user_half_year_newsids(uid)
    t2 = time.time()
    print 'time', t2 - t1
    print news_ids


if __name__ == '__main__':
    test1()
