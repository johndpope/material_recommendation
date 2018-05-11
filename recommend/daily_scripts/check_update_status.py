# encoding=utf8


'''
查看每天往solr里导入的数据是否有成功导入
'''

import time
import happybase

from general_utils.time_utils import timestamp2datetime
from general_utils.solr_utils import SolrQuery

from add_data_to_solr.cy_solr_local.solr_base import SolrCloud, ZooKeeper

solr_nat_online = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "news_and_topic")
# solr_nat_biz = SolrCloud(ZooKeeper("rd1:2181,rd2:2181"), "news_and_topic")

solr_utt_online = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "user_topn_topics")

solr_utn_online = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "user_topn_news")

# chunyu_search相关的表 all_online
solr_main_doctors = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "main_doctors")  # 这个每30min更新一次
solr_full_problem = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "full_problem")  # 每日凌晨3点多更新完
solr_robot_news = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "robot_news")  # 每日凌晨3点多更新完
solr_hospital_search = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "hospital_search")  # 每日凌晨3点多更新完
solr_pedia = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "pedia")  # 每日1点多更新完
solr_topics = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "topics")  # 每日1点多更新完
solr_problem_v2 = SolrCloud(ZooKeeper("md7:2181,md8:2181,md9:2181"), "problem_v2")  # 每日5点多更新完

EMAIL_CONFIG = {
    'server': "smtp.sina.com",
    'sender': "classify_test@sina.com",
    'password': "qwer1234",
    'receiver': ["yangxun@chunyu.me"],
    'port': 25,

}


def send_report_email(title, content):
    import smtplib
    from email.mime.text import MIMEText
    mail = MIMEText(content)
    mail['To'] = ';'.join(EMAIL_CONFIG["receiver"])
    mail['From'] = EMAIL_CONFIG["sender"]
    datestr = time.strftime('%Y-%m-%d')
    mail['Subject'] = title + '|' + str(datestr)
    try:
        smtpobj = smtplib.SMTP(EMAIL_CONFIG["server"], EMAIL_CONFIG["port"])
        smtpobj.login(EMAIL_CONFIG["sender"], EMAIL_CONFIG["password"])
        smtpobj.sendmail(EMAIL_CONFIG["sender"], EMAIL_CONFIG["receiver"], mail.as_string())
    except Exception, e:
        print e


def ask_solr(sort_field_name, solr):
    solrquery = SolrQuery()
    solrquery.set('q', '*:*')
    solrquery.set('sort', '%s desc' % sort_field_name)
    solrquery.set('rows', 1)
    solrquery.set('fl', [sort_field_name])

    res = [item for item in solr.search(**solrquery.get_query_dict())]

    last_value = res[0][sort_field_name]
    return {
        'last_value': last_value
    }


def get_cy_real_time_event_last_timestamp():
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=30000)
    table = connection.table("cy_real_time_event")
    max_ts = 0
    for key, value in table.scan():
        uid, ts, _ = key.split('|')
        ts = int(ts)
        if ts > max_ts:
            max_ts = ts
    return {
        'last_value': max_ts
    }


def format_content(res):
    last_value = res['last_value']
    return "最后时间戳 = %s" % timestamp2datetime(last_value / 1000)


def check_main_doctors():
    '''
    半小时查看一下main_doctor表的最后时间戳，若大于45min，则发送报警邮件
    :return:
    '''
    res = ask_solr("timestamp",solr_main_doctors)
    last_ts = res['last_value']
    now = time.time()
    if now - last_ts > 60 * 45 :#45min
        content_list = ["main_doctor太久没更新报警 " + format_content(res)]
        content = '\n'.join(content_list)
        title = '!!!!main_doctors坏了'
        send_report_email(title, content)


def main():
    content_list = []
    # 线上news_and_topic
    res0 = ask_solr('timestamp', solr_nat_online)
    content_list.append("线上news_and_topic " + format_content(res0))

    # # 测试news_and_topic
    # res1 = ask_solr('timestamp', solr_nat_biz)
    # content_list.append("测试news_and_topic " + format_content(res1))

    # 线上user_topn_topic
    res2 = ask_solr('timestamp', solr_utt_online)
    content_list.append("线上user_topn_topics " + format_content(res2))

    # 线上user_topn_news
    res3 = ask_solr('last_event_time', solr_utn_online)
    content_list.append("线上user_topn_news " + format_content(res3))

    # 线上main_doctors
    res4 = ask_solr('timestamp', solr_main_doctors)
    content_list.append("线上main_doctors " + format_content(res4))

    # full_problem
    res5 = ask_solr('timestamp', solr_full_problem)
    content_list.append("线上full_problem " + format_content(res5))

    # 罗伯特新闻
    res6 = ask_solr('timestamp', solr_robot_news)
    content_list.append("线上robot_news " + format_content(res6))

    # 豪斯披头搜索
    res7 = ask_solr('timestamp', solr_hospital_search)
    content_list.append("线上hospital_search " + format_content(res7))

    # pedia
    res8 = ask_solr('timestamp', solr_pedia)
    content_list.append("线上pedia " + format_content(res8))

    # topics
    res9 = ask_solr('timestamp', solr_topics)
    content_list.append("线上topics " + format_content(res9))

    # problem_v2
    res10 = ask_solr('timestamp', solr_problem_v2)
    content_list.append("线上problem_v2 " + format_content(res10))

    # cy_real_time_event
    res11 = get_cy_real_time_event_last_timestamp()
    content_list.append("cy_real_time_event " + format_content(res11))

    # 发送邮件
    content = '\n'.join(content_list)
    title = 'yx solr check report'
    send_report_email(title, content)


if __name__ == '__main__':
    import sys
    mode = sys.argv[1]
    if mode == 'daily':
        main()
    elif mode == 'main_doctor':
        check_main_doctors()
