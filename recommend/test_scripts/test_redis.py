# encoding=utf8

import sys
import os
import json

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
# import numpy as np
# from medweb_utils.util_app.redis_helper import persist_redis
from cy_cache.django_model.django_redis import get_redis_client_from_conf


# from settings import REDIS as DEFAULT_REDIS


class Word2VecCache(object):
    '''
    存word的vec和前30的similar words
    '''
    EXPIRATION = 60 * 60 * 24 * 2  # 两天过期
    # REDIS = persist_redis
    REDIS = get_redis_client_from_conf('material_redis')

    @classmethod
    def _build_key_vec(cls, word):
        return 'vec_%s' % word

    @classmethod
    def _build_key_similar(cls, word):
        return 'similar_%s' % word

    @classmethod
    def del_vec(cls, word):
        """
        删除对应的word vec
        """
        return cls.REDIS.delete(cls._build_key_vec(word))

    @classmethod
    def del_similar(cls, word):
        """
        删除对应的word vec
        """
        return cls.REDIS.delete(cls._build_key_similar(word))

    @classmethod
    def get_vec(cls, word):
        '''
        获取word的vec，没有返回[]
        :param word:
        :return:
        '''
        key = cls._build_key_vec(word)
        return cls.REDIS.get_pickle(key) or []

    @classmethod
    def set_vec(cls, word, vec):
        '''

        :param word:
        :param vec:
        :return:
        '''
        key = cls._build_key_vec(word)
        print key
        item = vec
        return cls.REDIS.setex_pickle(key, cls.EXPIRATION, item)

        # @classmethod
        # def set_reply(cls, pid, last_content_id, likely_reply_list, is_test=False):
        #     key = cls._build_key(pid)
        #     reply_item = {
        #         'last_content_id': last_content_id,
        #         'reply_list': likely_reply_list,
        #     }
        #     if is_test:
        #         print "reply_item", reply_item
        #         return
        #
        #     return cls.REDIS.setex_pickle(key, cls.EXPIRATION, reply_item)


def test():
    word = '哈哈'
    vec = [1, 2, 3, 4, 5]
    o = Word2VecCache.set_vec(word, vec)
    print o
    o2 = Word2VecCache.get_vec(word)
    print o2
    print type(o2)


if __name__ == '__main__':
    test()
