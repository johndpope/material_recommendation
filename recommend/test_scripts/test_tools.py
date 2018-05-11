#encoding=utf8
import sys
import os
import happybase
from general_utils.time_utils import ensure_second_timestamp,timestamp2datetime

def user_time_event(uid,begin,end):
    begin = ensure_second_timestamp(begin)
    end = ensure_second_timestamp(end)
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=30000)
    table = connection.table("cy_real_time_event")
    for key, value in table.scan(row_prefix=str(uid) + '|'):
        print "all key",key
        _,ts,event_type = key.split('|')
        ts = ensure_second_timestamp(ts)
        print "all time",timestamp2datetime(ts)
        if ts >= begin and ts <= end:
            print "shoot key",key,value
            print "shoot time",timestamp2datetime(ts)


def user_time_event2(uid,end,begin_list):
    #lookback_list = [5,10,15,20,30,60]必须是递增的整数
    #end = ensure_second_timestamp(end)
    #begin_list = [end - x * 61.0 for x in lookback_list]
    #end += 5.0
    connection = happybase.Connection('hbase_server', compat='0.90', port=19090, timeout=30000)
    table = connection.table("cy_real_time_event")
    rows = [item for item in table.scan(row_prefix=str(uid) + '|')]
    for key, value in rows[::-1]:
        _, ts, event_type = key.split('|')
        if event_type not in ("big_search","free_problem_create"):
            continue
        ts = ensure_second_timestamp(ts)
        if ts > end:#以后发生的事
            continue
        for i in range(len(begin_list)):
            begin = begin_list[i]

            #lookback = lookback_list[i]
            if ts >= begin:
                return i,event_type
    return len(begin_list),''


def get_problem_contents():
    from general_utils.db_utils import get_medicaldb_handler
    pid = sys.argv[2]
    sql = 'select content from ask_problemcontent where problem_id=%s;'%pid
    o = get_medicaldb_handler().dbhandler.do_one(sql)
    for item in o:
        print item



if __name__ == '__main__':

    fname = sys.argv[1]
    eval(fname+'()')
