# encoding=utf8
'''
用来分析log文件的脚本
'''

import sys

import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

import csv
import time
from collections import defaultdict

from general_utils.solr_utils import get_news_tags_from_solr
from general_utils.hbase_utils import cy_time_event_one_user_viewnews, cy_time_event_one_user_kernel
from general_utils.time_utils import datetime_str2timestamp, timestamp2datetime, get_today_timestamp, \
    ensure_second_timestamp, get_yesterday_date
from general_utils.text_utils import convert2gbk
from general_utils.file_utils import pickle_to_file

# from recommend.manager.data_helper import parse_user_info
from general_utils.db_utils import get_db_data_local_handler


def get_yesterday_log_filename():
    # medical01_20171123_info_logger.log
    yesterday = get_yesterday_date()
    dir_name = "/home/classify/workspace/material_recommendation/info_log/"
    prefix1 = "medical01_"
    prefix2 = "medical02_"
    log_file_name1 = dir_name + prefix1 + yesterday + "_info_logger.log"
    log_file_name2 = dir_name + prefix2 + yesterday + "_info_logger.log"

    ana_file_name1 = log_file_name1 + "_analysis.csv"
    ana_file_name2 = log_file_name2 + "_analysis.csv"

    bdp_file_name1 = log_file_name1 + "_analysis.bdpinput"
    bdp_file_name2 = log_file_name2 + "_analysis.bdpinput"

    bdp_file_name = dir_name + yesterday + "_info_log.bdpinput"

    return log_file_name1, ana_file_name1, bdp_file_name1, log_file_name2, ana_file_name2, bdp_file_name2, bdp_file_name


def main():
    log_file_name1, ana_file_name1, bdp_file_name1, log_file_name2, ana_file_name2, bdp_file_name2, bdp_file_name \
        = get_yesterday_log_filename()
    a1(log_file_name1, ana_file_name1, bdp_file_name1)
    a1(log_file_name2, ana_file_name2, bdp_file_name2)
    combine_bdpinput(bdp_file_name1, bdp_file_name2, bdp_file_name)


def combine_bdpinput(bdp_file_name1, bdp_file_name2, bdp_file_name):
    '''
    news_all_input_qa|||9017
    news_all_output_qa|||810
    news_no_info_qa|||0
    news_filtered_by_preprocessing_qa|||3659
    news_empty_res_qa|||1301
    news_bad_res_qa|||3203
    qa_failed|||86
    news_all_input_bs|||3988
    news_all_output_bs|||1515
    news_no_info_bs|||110
    news_filtered_by_preprocessing_bs|||1589
    news_empty_res_bs|||138
    news_bad_res_bs|||666
    bs_failed|||32
    '''
    KEY_EXCLUDE = ("qa_failed", "bs_failed")

    cnt_dict = defaultdict(int)
    for x in (bdp_file_name1, bdp_file_name2):
        with open(x, 'r') as f:
            for l in f:
                key, cnt = l.strip('\n').split('|||')
                if key in KEY_EXCLUDE:
                    continue

                cnt = int(cnt)
                cnt_dict[key] += cnt
    pickle_to_file(cnt_dict, bdp_file_name)


def a1(log_file_name, ana_file_name, bdp_file_name):
    START = "==========start======="
    # 2017 11 08 日，被成功推荐的，用户uid，触发时间，推送文章id，推送时间，
    # 当天推送时间后是否浏览过该文章，以及浏览时间

    today_zero, today_end = get_today_timestamp(time.time() - 86400.0)

    def get_uid(l):
        return l.split("=uid=")[1].split('=')[0]

    fi = open(log_file_name, 'r')
    fo = open(ana_file_name, "w")
    csvwriter = csv.writer(fo, dialect='excel')
    first_line = [u"uid", u"触发时间", u"触发类型", u"用户全文", u"用户tag", u"用户人群",
                  u"文章id", u"文章标题", u"文章tag", u"文章分类", u"返回时间", u"点击时间"]
    csvwriter.writerow(convert2gbk(first_line))
    uid = None
    uni_key0 = None
    trigger_time = None
    trigger_type = None
    caled = set()
    all = set()  # 所有触发的请求
    reason = None

    all_qa = defaultdict(set)
    all_bs = defaultdict(set)
    cnt = 0
    for l in fi:
        # if not l.startswith("2017-11-08"):
        #     continue
        cnt += 1
        # if cnt > 10000:
        #     continue

        if START in l:
            # 记录上一个
            if reason and uni_key0 and trigger_type:
                if trigger_type == "bs":
                    all_bs["all"].add(uni_key0)
                elif trigger_type == "qa":
                    all_qa["all"].add(uni_key0)

            if uni_key0 and trigger_type and not reason:
                if trigger_type == "bs":
                    all_bs["failed"].add(uni_key0)
                elif trigger_type == "qa":
                    all_qa["failed"].add(uni_key0)

            uid = get_uid(l)
            trigger_time = l.split(',')[0]
            uni_key0 = uid + '|' + trigger_time

            if "pid=None" in l:
                trigger_type = "bs"

            else:
                trigger_type = "qa"
                all_qa["all"].add(uni_key0)

            reason = None

            # all.add(uni_key0)

            trigger_ts = datetime_str2timestamp(trigger_time)
            print "uni_key", uni_key0
            print "ts", trigger_ts

        # if "=trigger=" in l:
        #     trigger_type0 = l.split("=trigger=")[1].split('=')[0]

        if "=special_population=" in l:
            special_population0 = l.split("=special_population=")[1].split("=")[0]

        if "=texts=" in l:
            texts0 = l.split("=texts=")[1].split("=")[0]

        if "=tags=" in l:
            tags0 = l.split("=tags=")[1].split("=")[0]

        if "failed in recommend==" in l:
            reason = l.split("failed in recommend==")[1].split("=")[0]
            if trigger_type == "qa":

                if reason not in all_qa:
                    all_qa[reason] = set([uni_key0])
                else:
                    all_qa[reason].add(uni_key0)
            elif trigger_type == "bs":

                if reason not in all_bs:
                    all_bs[reason] = set([uni_key0])
                else:
                    all_bs[reason].add(uni_key0)

        if "succeed in recommend==========" in l:
            reason = "succeed"
            if trigger_type == "qa":
                if reason not in all_qa:
                    all_qa[reason] = set([uni_key0])
                else:
                    all_qa[reason].add(uni_key0)
            elif trigger_type == "bs":
                if reason not in all_bs:
                    all_bs[reason] = set([uni_key0])
                else:
                    all_bs[reason].add(uni_key0)

            return_time = l.split(',')[0]
            uni_key = uid + return_time
            if uni_key in caled:
                continue
            print 'WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW'

            caled.add(uni_key)
            return_ts = datetime_str2timestamp(return_time)
            nid = l.split("=====id=")[1].split("=")[0]
            ntitle = l.split("===title=")[1].split("=")[0]

            # news_title = get_db_data_local_handler().get_news_title(nid)
            news_type = get_db_data_local_handler().get_news_type(nid)
            news_tags = u'|||'.join(get_news_tags_from_solr("news_" + str(nid)))

            print uid
            print trigger_type

            print nid
            print return_ts
            print type(ntitle)

            # first_line = [u"uid", u"触发时间", u"触发类型",u"用户全文",u"用户tag",u"用户人群",
            #    u"文章id", u"文章标题",u"文章tag",u"文章分类",u"返回时间", u"点击时间"]
            views = cy_time_event_one_user_viewnews(uid, begin=return_ts, end=today_end)
            print views

            rows = [str(uid), trigger_time, trigger_type, texts0, tags0, special_population0,
                    str(nid), ntitle, news_tags, news_type, return_time,
                    str(timestamp2datetime(views.get(nid, -1)))]
            rows = convert2gbk(rows)

            csvwriter.writerow(rows)

    csvwriter.writerow([u"所有uid".encode("gbk"), u"推了的uid".encode("gbk")])
    rows = [str(len(all)), str(len(caled))]
    rows = convert2gbk(rows)
    csvwriter.writerow(rows)

    fi.close()
    fo.close()

    for x in all_qa:
        print x + "|||" + str(len(all_qa[x]))

    for x in all_bs:
        print x + "|||" + str(len(all_bs[x]))

    with open(bdp_file_name, "w") as f:
        f.write("news_all_input_qa|||" + str(len(all_qa["all"])) + "\n")
        f.write("news_all_output_qa|||" + str(len(all_qa["succeed"])) + "\n")
        f.write("news_no_info_qa|||" + str(len(all_qa["user_info is None "])) + "\n")
        f.write("news_filtered_by_preprocessing_qa|||" + str(len(all_qa["filter_user_info bad "])) + "\n")
        f.write("news_empty_res_qa|||" + str(len(all_qa["topn_ids_scores empty"])) + "\n")
        f.write("news_bad_res_qa|||" + str(len(all_qa["best_score so low"])) + "\n")
        f.write("qa_failed|||" + str(len(all_qa["failed"])) + "\n")

        f.write("news_all_input_bs|||" + str(len(all_bs["all"])) + "\n")
        f.write("news_all_output_bs|||" + str(len(all_bs["succeed"])) + "\n")
        f.write("news_no_info_bs|||" + str(len(all_bs["user_info is None "])) + "\n")
        f.write("news_filtered_by_preprocessing_bs|||" + str(len(all_bs["filter_user_info bad "])) + "\n")
        f.write("news_empty_res_bs|||" + str(len(all_bs["topn_ids_scores empty"])) + "\n")
        f.write("news_bad_res_bs|||" + str(len(all_bs["best_score so low"])) + "\n")
        f.write("bs_failed|||" + str(len(all_bs["failed"])) + "\n")


if __name__ == '__main__':
    main()
