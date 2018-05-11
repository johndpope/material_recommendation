#encoding=utf8

import os

from general_utils.time_utils import get_yesterday_date
from recommend.app_config import USER_TOPN_NEWS_DATADIR, USER_TOPN_TOPIC_DATADIR

yesterday_date = get_yesterday_date()


def get_parti_uid_filename(part, mode):
    if mode == 'news':
        data_dir = USER_TOPN_NEWS_DATADIR
    elif mode == 'topic':
        data_dir = USER_TOPN_TOPIC_DATADIR
    else:
        print 'mode in ("news","topic")'
        return ''

    return os.path.join(data_dir, yesterday_date + '.uid%s' % part)


def get_parti_solr_filename(part, mode):
    if mode == 'news':
        data_dir = USER_TOPN_NEWS_DATADIR
    elif mode == 'topic':
        data_dir = USER_TOPN_TOPIC_DATADIR
    else:
        print 'mode in ("news","topic")'
        return ''
    return os.path.join(data_dir, yesterday_date + '.solr%s' % part)



