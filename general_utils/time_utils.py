# encoding=utf8

import time
import datetime


def get_time_stamp():
    return int(round(time.time()))


def getTimeOClockOfToday(timestamp=None):
    if timestamp:

        t = time.localtime(timestamp)
    else:
        t = time.localtime(time.time())

    time1 = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t), '%Y-%m-%d %H:%M:%S'))

    return long(time1)


def get_today_timestamp(timestamp=None):
    today_zero = getTimeOClockOfToday(timestamp)
    today_over = today_zero + 86400.0
    return today_zero, today_over


def get_yesterday_timestamp(ts=None):
    today_zero, today_over = get_today_timestamp(ts)
    return today_zero - 86400.0, today_over - 86400.0


def get_ndays_before_timestamp(n, timestamp=None):
    today_zero = getTimeOClockOfToday(timestamp)
    ndays_before_zero = today_zero - n * 86400.0
    return ndays_before_zero


def get_48h_timestamps(timestamp=None):
    today_zero = getTimeOClockOfToday(timestamp)
    two_days_before_zero = today_zero - 86400 * 2
    return two_days_before_zero, today_zero


def half_year_before_timestamp(timestamp=None):
    if not timestamp:
        timestamp = time.time()
    else:
        timestamp = float(timestamp)
    return timestamp - 86400.0 * 365 / 2


def timestamp2datetime(timestamp):
    # 将时间戳转化为这样事儿的-》2017-10-25 16:13:41，后者用于mysql中的比较
    time_local = time.localtime(timestamp)
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
    return dt


def timestamp2date(timestamp):
    time_local = time.localtime(timestamp)
    return time.strftime("%Y%m%d", time_local)


def timestamp2date2(timestamp):
    time_local = time.localtime(timestamp)
    return time.strftime("%Y-%m-%d", time_local)


def timestamp2date3(timestamp):
    time_local = time.localtime(timestamp)
    return myDate(time_local.tm_year, time_local.tm_mon, time_local.tm_mday).get_unique_str_key()


def datetime_str2datetime(datetime_str):
    ds = str(datetime_str)
    dt = datetime.datetime.strptime(ds, '%Y-%m-%d %H:%M:%S')
    return dt


def datetime_str2timestamp(dt_str):
    dt = datetime_str2datetime(dt_str)
    time_tuple = dt.timetuple()
    ts = time.mktime(time_tuple)
    return ts


class myDate(object):
    def __init__(self, year=0, month=0, day=0):
        self.year = year
        self.month = month
        self.day = day
        self.key_tuple = (year, month, day)

    def equal(self, date1):
        return self.key_tuple == date1.key_tuple

    def get_unique_str_key(self):
        return '-'.join([str(self.year), str(self.month), str(self.day)])

    def add_n_day(self, n):
        '''
        增加n天
        :param n:
        :return:
        '''
        d0 = datetime.datetime(self.year, self.month, self.day)
        d1 = d0 + datetime.timedelta(n)
        self.year = d1.year
        self.month = d1.month
        self.day = d1.day

    @classmethod
    def recover_date(cls, unique_str_key):
        year, month, day = unique_str_key.split('-')
        return myDate(int(year), int(month), int(day))


def get_yesterday_date(now=None):
    if not now:
        now = time.time()
    now = ensure_second_timestamp(now)
    yesterday = now - 86400.0  # 昨天
    return timestamp2date(yesterday)


def ensure_second_timestamp(timestamp):
    if isinstance(timestamp, basestring) and '-' in timestamp:
        timestamp = datetime_str2timestamp(timestamp)
    ts_int_str = str(int(float(timestamp)))
    if len(ts_int_str) == 13:
        return float(timestamp) / 1000.0
    return float(timestamp)


def ensure_m_timestamp(timestamp):
    if isinstance(timestamp, basestring) and '-' in timestamp:
        timestamp = datetime_str2timestamp(timestamp)
    ts_int_str = str(int(float(timestamp)))
    if len(ts_int_str) == 10:
        return int(timestamp * 1000)
    return int(timestamp)


def test_myDate():
    date = myDate(2018,2,28)
    date.add_n_day(-3)
    print date.get_unique_str_key()

print timestamp2datetime(ensure_second_timestamp(1522641002482))
print datetime_str2timestamp("2018-03-09 7:0:0")
print timestamp2date3(1520261533)
test_myDate()
# print get_yesterday_date()
# print time.time()
