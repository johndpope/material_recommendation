# encoding=utf8
from __future__ import absolute_import
import json
from rpc_services import rpc_clients


def test():
    uids = ['', '103984463']
    input_arg = json.dumps(
        {
            'user_ids': uids,
            'fields': ['username']
        }
    )

    o = json.loads(rpc_clients.get_user_profile_client().getUserProfileInfo(input_arg))
    print o


def get_username(uid):
    uid = str(uid)
    uids = [uid]
    input_arg = json.dumps(
        {
            'user_ids': uids,
            'fields': ['username']
        }
    )
    try:
        o = json.loads(rpc_clients.get_user_profile_client().getUserProfileInfo(input_arg))
        if uid not in o:
            return ''
        return o[uid]['username']
    except Exception, e:
        print 'user profile get user name exception', e
        return ''


def is_app_user(uid):
    username = get_username(uid)
    if not username or u'@' in username or u'pedo_' in username:
        return False
    return True


if __name__ == '__main__':
    test()
