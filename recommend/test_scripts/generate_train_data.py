# encoding=utf8
'''
①获取(uid, ts, news_id)点击行为的tuple
②获取一段时间的科普feed流展示列表
2017-10-31 0 0 0 ts: 1509379200
2018-02-28 0 0 0 ts: 1519747200


'''

import os
import sys
import csv
import json
import time
from random import shuffle
from collections import defaultdict

import happybase

from global_config import get_root_path

from general_utils import pysolr
from general_utils.math_utils import yield_batch_data
from general_utils.solr_utils import SolrQuery
from general_utils.time_utils import ensure_second_timestamp, ensure_m_timestamp, \
    timestamp2date, datetime_str2timestamp, myDate, timestamp2date3
from general_utils.db_utils import get_newsdb_handler
from general_utils.file_utils import pickle_to_file, pickle_from_file, read_from_xlsx_file

data_dir = os.path.join(get_root_path(),
                        'recommend',
                        'data_dir',
                        'train_data',
                        )
showlist_filename = os.path.join(data_dir, 'showlist')
showlist_filename_120 = os.path.join(data_dir, 'showlist_120_day_from_server.xlsx')
solr_rowkeylist_filename = os.path.join(data_dir, 'rowkeys')
click_actions_filename = os.path.join(data_dir, 'clickations')
showlist_parsed_filename = os.path.join(data_dir, 'parsed_showlist.pkl')
raw_train_data_filename = os.path.join(data_dir, 'raw_train_data')
user_basic_info_filename = os.path.join(data_dir, 'user_basic_info')
news_basic_info_filename = os.path.join(data_dir, 'news_basic_info')
train_data_filename = os.path.join(data_dir, 'train.data')
test_data_filename = os.path.join(data_dir, 'test.data')

qa_ask_path = os.path.join(data_dir, 'qa_ask')

if not os.path.exists(data_dir):
    os.makedirs(data_dir)

zk_md4 = pysolr.ZooKeeper('md4:2181',
                          ['news_profile'])

solr_np = pysolr.SolrCloud(zk_md4, 'news_profile', timeout=120)  # news_profile


def get_feed_showlist_dict(begin, end):
    '''
    获取begin和end之间每天的展示列表，并存为date：news_list的字典
    :param begin: 精确到秒的时间戳
    :param end: 精确到秒的时间戳
    :return:
    '''
    view_time_th = 5000  # 5000以下认为没展示过，5000以上认为被展示过
    # 调整时间戳格式
    begin = ensure_second_timestamp(begin)
    end = ensure_second_timestamp(end)

    # begin , end 转换成日期
    begin_d = timestamp2date(begin)
    end_d = timestamp2date(end)

    # 从数据库select
    sql = 'select id,date,view_times from news_healthnews where is_online=1 and date<="%s" and date>="%s";' % (
        end_d, begin_d)

    o = get_newsdb_handler().dbhandler.do_one(sql)

    date_newsid_dict = defaultdict(list)
    for item in o:
        news_id = int(item[0])
        date = item[1]
        view_times = int(item[2])
        if view_times < view_time_th:
            continue
        date_newsid_dict[date].append(news_id)

    return date_newsid_dict


def get_feed_showlist_data():
    '''
    使用一年的数据吧（2017.01.01-2018.01.01）
    :return:
    '''
    begin = datetime_str2timestamp('2017-01-01 0:0:0')
    end = datetime_str2timestamp('2018-01-01 0:0:0')
    date_newsid_dict = get_feed_showlist_dict(begin, end)
    pickle_to_file(date_newsid_dict, showlist_filename)


def get_views_news_rowskeys_from_solr(begin, end):
    begin = ensure_m_timestamp(begin)
    end = ensure_m_timestamp(end)
    ###################


def get_click_data():
    '''
    （2017.10.31-2018.02.27）120天
    分批从solr获取rowkey_list,每天一批（几十万）
    :return:
    '''
    begin = 1509379200
    end = 1519747200

    # output file
    row_key_list_file = open(solr_rowkeylist_filename, 'w')

    for i in range((end - begin) / 86400):
        begin_i = begin + i * 86400
        end_i = begin_i + 86400
        begin_i_m = int(begin_i * 1000)
        end_i_m = int(end_i * 1000)
        print 'begin_i_m', begin_i_m, 'end_i_m', end_i_m
        # 构造query
        solrquery = SolrQuery()
        q = '*:*'
        solrquery.set('q', q)
        solrquery.add('fq', 'event_time:[%s TO %s]' % (begin_i_m, end_i_m))
        solrquery.set('fl', ['id'])
        solrquery.set('rows', 100000)

        rowkey_list = [item['id'] for item in solr_np.search(**solrquery.get_query_dict())]
        row_key_list_file.write('\n'.join(rowkey_list))
        row_key_list_file.write('\n')
        print '%s row keys are saved' % len(rowkey_list)
    row_key_list_file.close()

    # output file2
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=30000)
    table = connection.table("cy_event")

    click_action_file = open(click_actions_filename, 'w')
    row_key_list_file = open(solr_rowkeylist_filename, 'r')

    data_lines = row_key_list_file.readlines()
    data_lines = [item.strip('\n') for item in data_lines]
    row_key_list_file.close()
    cols = ['info:uid', 'info:news_id']
    for batch_data in yield_batch_data(data_lines, 1000):

        row = table.rows(batch_data, columns=cols)
        for key, value in row:
            uid = int(value['info:uid'])
            newsid = int(value['info:news_id'])
            a, ts, b = key.split('|')
            outputstr = '|'.join([str(uid), str(ts), str(newsid)]) + '\n'
            click_action_file.write(outputstr)
    click_action_file.close()
    connection.close()


def get_line_info(line):
    bad_return = None, None
    try:
        id = int(line[0])
    except:
        return bad_return
    o_date_str = line[6]

    if u"前" in o_date_str:
        date = myDate(2018, 2, 27)
    elif u"月" in o_date_str:
        year = 2018
        month = int(o_date_str.split(u"月")[0])
        day = int(o_date_str.split(u"月")[1].split(u"日")[0])
        date = myDate(year, month, day)
    else:
        year, month, day = o_date_str.split('-')
        year = int(year)
        month = int(month)
        day = int(day)
        date = myDate(year, month, day)

    return id, date.get_unique_str_key()


def parse_showlist_file():
    '''
    ***假设用户会翻看两天的展示列表，保证一组正负例所有的日期戳是同一天
    则 ① 当天新展示的最上面一条，没有后一天的负例
       ② 假设过了一天，当天（d0)的展示的每一条，日期戳应该往后加一天(d1)，这时有后一天的负例，负例的时间戳为后一天(d1)
    :return:
    '''
    data_lines = read_from_xlsx_file(showlist_filename_120)
    res = []
    for index, line in enumerate(data_lines):
        if index > 0:
            up_line = data_lines[index - 1]
        else:
            up_line = [''] * 6
        if index < len(data_lines) - 1:
            down_line = data_lines[index + 1]
        else:
            down_line = [''] * 6

        up_id, up_date_str = get_line_info(up_line)
        id, date_str = get_line_info(line)
        down_id, down_date_str = get_line_info(down_line)
        print '==' * 20
        print "up_id, up_date_str", up_id, up_date_str
        print "id, date_str ", id, date_str
        print "down_id, down_date_str", down_id, down_date_str

        if id is None:
            continue

        is_first = (up_date_str != date_str)

        # 当天正负例
        positive_key = str(id) + '|' + date_str
        if is_first:
            up_negative_key = ''
        else:
            up_negative_key = str(up_id) + '|' + date_str
        if down_id:
            down_negative_key = str(down_id) + '|' + date_str
        else:
            down_negative_key = ''

        str0 = '#'.join([positive_key, up_negative_key, down_negative_key])

        # 次日正负例
        y, m, d = date_str.split('-')
        date = myDate(int(y), int(m), int(d))
        date.add_n_day(1)
        next_date_str = date.get_unique_str_key()
        next_positive_key = str(id) + '|' + next_date_str
        if up_id:
            next_up_negative_key = str(up_id) + '|' + next_date_str
        else:
            next_up_negative_key = ''
        if down_id:
            next_down_negative_key = str(down_id) + '|' + next_date_str
        else:
            next_down_negative_key = ''
        str1 = '#'.join([next_positive_key, next_up_negative_key, next_down_negative_key])

        res.append(str0)
        res.append(str1)

    with open(showlist_parsed_filename, 'w') as fo:
        fo.write('\n'.join(res))


def generate_raw_train_data():
    '''
    uid|news_id|ts|is_click
    :return:
    '''
    # 加载展示列表及上下文信息
    showlist_data = {}
    with open(showlist_parsed_filename, 'r') as f:
        for l in f:
            l = l.strip('\n')
            positive_key, negative_key0, negative_key1 = l.split('#')
            showlist_data[positive_key] = [negative_key0, negative_key1]

    # output file
    raw_train_data_file = open(raw_train_data_filename, 'w')

    # 遍历click_action_file
    cnt_good = 0
    cnt_all = 0
    user_click = defaultdict(list)  # uid:[120007|2018-2-28,...]
    with open(click_actions_filename, 'r') as f:
        for l in f:
            cnt_all += 1

            # 126451997|1509379220841|111851
            l = l.strip('\n')
            uid, ts_m, news_id = l.split('|')
            uid = int(uid)
            ts_m = int(ts_m)
            # 获取时间戳的date
            unique_date_str = timestamp2date3(ts_m / 1000.0)
            key = news_id + '|' + unique_date_str
            user_click[uid].append(key)

    print 'len user_click dict', len(user_click)

    # 遍历uid
    for uid in user_click:
        user_clicks = user_click[uid]  # 正例们

        for positive in user_clicks:

            if positive in showlist_data:
                user_not_clicks = []  # 负例
                negative0, negative1 = showlist_data[positive]
                if negative0 and negative0 not in user_clicks:
                    user_not_clicks.append(negative0)
                if negative1 and negative1 not in user_clicks:
                    user_not_clicks.append(negative1)
                # 写入正例
                # uid|news_id|ts|label
                raw_train_data_file.write('|'.join([str(uid), positive, '1']) + '\n')
                # 写入负例
                for negative in user_not_clicks:
                    raw_train_data_file.write('|'.join([str(uid), negative, '0']) + '\n')
    raw_train_data_file.close()

    print cnt_good, cnt_all


def get_user_base_info():
    # from hbase cy_users
    cols = ['info:gold_coin', 'info:username',
            'info:city', 'info:ehr_sex', 'info:balance',
            'info:province']

    def get_all_uids():
        raw_train_data_file = open(raw_train_data_filename, 'r')
        ls = raw_train_data_file.readlines()
        raw_train_data_file.close()
        all_uids = set()
        for l in ls:
            uid = l.split('|')[0]
            all_uids.add(uid)
        return all_uids

    all_uids = get_all_uids()
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=30000)
    table = connection.table("cy_user")

    user_basic_info_file = open(user_basic_info_filename, 'w')

    for batch_data in yield_batch_data(list(all_uids)):
        o = table.rows(batch_data, columns=cols)
        for key, value in o:
            jsonobj = json.dumps(
                {
                    'uid': key,
                    'info': value,
                }
            )
            user_basic_info_file.write(jsonobj + '\n')
    user_basic_info_file.close()


def get_news_base_info():
    from general_utils.db_utils import get_newsdb_handler

    def get_all_news_ids():
        all_news_ids = set()
        with open(raw_train_data_filename, 'r') as f:
            ls = f.readlines()
        for l in ls:
            all_news_ids.add(l.split('|')[1])
        return list(all_news_ids)

    all_news_ids = get_all_news_ids()

    news_basic_info_file = open(news_basic_info_filename, 'w')
    field_list = [
        'news_type',
        'favor_num',
        'view_times',
        'very_favor_num',
        'share_num'
    ]
    field_str = ','.join(field_list)
    for news_id in all_news_ids:
        sql = 'select %s from news_healthnews where id=%s;' % (field_str, news_id)
        mysql_output = get_newsdb_handler().do_one(sql)
        if mysql_output:
            info_dict = dict(zip(field_list, mysql_output[0]))
            info_dict['news_id'] = news_id
            jsonobj = json.dumps(info_dict)
            news_basic_info_file.write(jsonobj + '\n')

    news_basic_info_file.close()


def train_data():
    from rpc_services.user_profile_api import is_app_user

    def load_user_basic_info():
        user_basic_info_dict = {}
        with open(user_basic_info_filename, 'r') as f:
            for l in f:
                l = json.loads(l)
                uid = l['uid']
                info = l['info']
                user_basic_info_dict[uid] = info
        return user_basic_info_dict

    def load_news_basic_info():
        news_basic_info_dict = {}
        with open(news_basic_info_filename, 'r') as f:
            for l in f:
                l = json.loads(l)
                news_id = l['news_id']
                news_basic_info_dict[news_id] = l
        return news_basic_info_dict

    user_basic_info_dict = load_user_basic_info()
    news_basic_info_dict = load_news_basic_info()

    train_data_set = []

    with open(raw_train_data_filename, 'r') as f:
        for l in f:
            # 130023449 | 114811 | 2017 - 12 - 1 | 0
            # user_features :
            #       [uid(int),is_app_user(cate),sex(cate),gold_coin(int),balence(int),city(cate),province(cate)]
            # news_features :
            #       [news_id(int), news_type(cate), view_times(int), favor_num(int),very_favor_num(int),share_num(int)]
            uid, nid, day, label = l.strip('\n').split('|')
            print uid, nid, day, label
            uid = unicode(uid)
            nid = unicode(nid)
            user_basic_info = user_basic_info_dict[uid]
            news_basic_info = news_basic_info_dict[nid]
            user_features = [uid, is_app_user(user_basic_info.get('info:username', '')),
                             user_basic_info.get('info:ehr_sex', '?'),
                             user_basic_info.get('info:gold_coin', '0'),
                             user_basic_info.get('info:balance', '0'),
                             user_basic_info.get('info:city', '?'),
                             user_basic_info.get('info:province', '?')]

            news_features = [
                nid, news_basic_info.get('news_type', '?'),
                news_basic_info.get('view_times', '0'),
                news_basic_info.get('favor_num', '0'),
                news_basic_info.get('very_favor_num', '0'),
                news_basic_info.get('share_num', '0')
            ]

            features = user_features + news_features + [label]
            features = [str(x).replace(',', '').replace(' ', '') for x in features]
            train_data_set.append(', '.join(features))
    shuffle(train_data_set)
    test_num = 10000
    train_set = train_data_set[:-test_num]
    test_set = train_data_set[-test_num:]
    with open(train_data_filename, 'w') as f:
        for x in train_set:
            f.write(x + '\n')

    with open(test_data_filename, 'w') as f:
        for x in test_set:
            f.write(x + '\n')


def look_citys_and_provinces():
    citys = set()
    provinces = set()
    with open(user_basic_info_filename, 'r') as f:
        for l in f:
            info = json.loads(l)['info']
            city = info.get('info:city', 'xx')
            province = info.get('info:province', 'xxx')
            citys.add(city)
            provinces.add(province)

    for x in citys:
        print 'city', x
    print len(citys)
    print '-' * 20
    for x in provinces:
        print 'province', x
    print len(provinces)


#################################################################
# 特殊特征
def get_one_year_qa_ask():
    '''
    为了统计词与科室的关系，先把语料（qa首问搞下来存起来）
    :return:
    '''
    import json
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=30000)
    table = connection.table("problem")
    start = '2017-01-01'
    end = '2018-01-01'
    t1 = time.time()
    cnt = 0
    fo = open('2017-2018_qa_ask', 'w')
    for key, value in table.scan(row_start=start, row_stop=end):

        # print json.dumps(value)
        c_key = "detail:clinic_no"
        if c_key not in value:
            continue
        clinic_no = value[c_key]
        if clinic_no == 'null':
            continue
        ask = value['detail:ask']
        fo.write(json.dumps({
            'key': key,
            'ask': ask,
            'clinic_no': clinic_no,
        }) + '\n')
    fo.close()
    t2 = time.time()
    print 'two month time', t2 - t1


def seg_ask():
    part = sys.argv[1]
    import jieba
    import json
    medical_vocab_file = 'medical_vocabulary'
    jieba.load_userdict(medical_vocab_file)

    def get_filename(part):
        return os.path.join(qa_ask_path, 'part-%s' % part)

    all_medical_words = []
    with open(medical_vocab_file, 'r') as fm:
        for l in fm:
            l = unicode(l.strip('\n'))
            all_medical_words.append(l)

    fin = get_filename(part)
    fou = fin + '_seg'
    with open(fin, 'r') as fi, open(fou, 'w') as fo:
        for line in fi:
            data = json.loads(line.strip('\n'))
            ask = data['ask']
            clinic_no = data['clinic_no']
            word_list = [x for x in jieba.cut(ask) if x in all_medical_words]
            for x in word_list:
                key = x + '|||' + clinic_no
                fo.write(key + '\n')


def test():
    p = pickle_from_file(showlist_parsed_filename)
    for key in p:
        before_key = p[key].get('before_key', None)
        after_key = p[key].get('after_key', None)
        print '*' * 30
        print 'before key', before_key
        print 'this key', key
        print 'after key', after_key


if __name__ == '__main__':
    train_data()
