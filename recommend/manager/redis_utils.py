# encoding=utf8

import sys
import os
import json

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

from cy_cache.django_model.django_redis import get_redis_client_from_conf

NOT_IN_WORD2VEC_SIGN = 1  # 不能设置为[],0....
NOT_IN_REDIS_SIGN = 2
NOT_IN_WORD2VEC_SIGN_API = []  # api返回的没有向量的词的标记

NOT_IN_WORD2VEC_SIMILAR_SIGN = 1
NOT_IN_WORD2VEC_SIMILAR_SIGN_API = []


# 注： NOT_IN_WORD2VEC_SIGN redis存的api非向量词标记，设置为1了，否则存不上,和NOT_IN_WORD2VEC_SIGN_API不一样


class Word2VecCache(object):
    '''
    存word的vec和前30的similar words
    '''
    EXPIRATION = 60 * 60 * 24 * 2  # 两天过期
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

    # @classmethod
    # def super_delete(cls):
    #     print len(cls.REDIS._db._ex_keys)

    @classmethod
    def del_similar(cls, word):
        """
        删除对应的word vec
        """
        return cls.REDIS.delete(cls._build_key_similar(word))

    @classmethod
    def get_similar(cls, word, num=30):
        '''
        获取word的相似词，最多30个
        '''
        key = cls._build_key_similar(word)
        try:
            similar_word_list = cls.REDIS.get_pickle(key) or NOT_IN_REDIS_SIGN
        except Exception, e:
            print 'get similar exception', e
            similar_word_list = NOT_IN_REDIS_SIGN
        if similar_word_list == NOT_IN_WORD2VEC_SIMILAR_SIGN:
            similar_word_list = NOT_IN_WORD2VEC_SIMILAR_SIGN_API
        if isinstance(similar_word_list, list):
            return similar_word_list[:num]
        return similar_word_list

    @classmethod
    def get_similar_list(cls, word_list, num=30):
        '''
        get_similar的list版
        :param word_list:
        :param num:
        :return:
        '''
        return [cls.get_similar(word, num) for word in word_list]

    @classmethod
    def set_similar(cls, word, similar_word_list):
        '''

        :param word:
        :param similar_word_list:
        :return:
        '''
        key = cls._build_key_similar(word)
        if similar_word_list == NOT_IN_WORD2VEC_SIMILAR_SIGN_API:
            similar_word_list = NOT_IN_WORD2VEC_SIMILAR_SIGN
        item = similar_word_list
        # print 'set similar',item,type(similar_word_list)
        try:
            return cls.REDIS.setex_pickle(key, cls.EXPIRATION, item)
        except Exception, e:
            print 'set similar exception', e
            return None

    @classmethod
    def get_vec(cls, word):
        '''
        获取word的vec，没有返回NOT_IN_REDIS_SIGN
        :param word:
        :return:
        '''
        key = cls._build_key_vec(word)
        try:
            vec = cls.REDIS.get_pickle(key) or NOT_IN_REDIS_SIGN
        except Exception, e:
            print 'redis exception', e
            vec = NOT_IN_REDIS_SIGN
        if vec == NOT_IN_WORD2VEC_SIGN:
            vec = NOT_IN_WORD2VEC_SIGN_API
        return vec

    @classmethod
    def get_vec_list(cls, words):
        '''
        get_vec 的 list 版本
        :param words:
        :return:
        '''
        return [cls.get_vec(word) for word in words]

    @classmethod
    def set_vec(cls, word, vec):
        '''

        :param word:
        :param vec:
        :return:
        '''
        key = cls._build_key_vec(word)
        if vec == NOT_IN_WORD2VEC_SIGN_API:
            vec = NOT_IN_WORD2VEC_SIGN

        item = vec
        # print 'set_vec', 'key=', key, 'item=', item
        try:
            return cls.REDIS.setex_pickle(key, cls.EXPIRATION, item)
        except Exception, e:
            print 'redis set vec exception', e
            return None

    @classmethod
    def set_vec_list(cls, word_list, vec_list):
        '''
        set_vec 的 list 版本
        不在word2vec里的vec设置为NOT_IN_WORD2VEC_SIGN
        :param word_list:
        :param vec_list:
        :return:
        '''
        if len(word_list) != len(vec_list):
            return []
        return [cls.set_vec(word_list[i], vec_list[i]) for i in range(len(word_list))]

    @classmethod
    def _build_key_user_showed_news(cls, uid):
        uid = str(uid)
        return 'userShowedNews_%s' % (uid)

    @classmethod
    def set_user_showed_news(cls, uid, news_ids):
        if not news_ids:
            return False

        key = cls._build_key_user_showed_news(uid)
        item = '|'.join([str(x) for x in news_ids])
        try:
            cls.REDIS.setex_pickle(key, cls.EXPIRATION, item)
            return True
        except Exception, e:
            print "set_user_showed_news exception", e
            return False

    @classmethod
    def del_user_shows_news(cls, uid):

        return cls.REDIS.delete(cls._build_key_user_showed_news(uid))


    @classmethod
    def get_user_showed_news(cls, uid):
        key = cls._build_key_user_showed_news(uid)
        try:
            id_str_list = cls.REDIS.get_pickle(key)
            return [int(x) for x in id_str_list.split('|')]
        except Exception, e:
            print "get_user_showed_news", e
            return []

    @classmethod
    def _build_key_user_all_news(cls, uid):
        return 'userAllNews_%s' % uid

    @classmethod
    def set_user_all_news(cls, uid, news_id_list):
        if not news_id_list:
            return False
        key = cls._build_key_user_all_news(uid)
        item = json.dumps(news_id_list)
        try:
            cls.REDIS.setex_pickle(key, cls.EXPIRATION, item)
            return True
        except Exception, e:
            print "set_user_all_news exception", e
            return False

    @classmethod
    def get_user_all_news(cls, uid):
        key = cls._build_key_user_all_news(uid)
        try:
            id_str_list = cls.REDIS.get_pickle(key)
            return json.loads(id_str_list)
        except Exception, e:
            print "get_user_all_news exception", e
            return []


def test():
    uid = 11111
    ids = [[111, 2, 3],[3,4,5],[6,7,8]]
    print 'test 1 uid %s ids %s' % (uid, ids)
    Word2VecCache.set_user_all_news(uid, ids)
    o = Word2VecCache.get_user_all_news(uid)
    print o, type(o)
    print "test 2 id uid %s not in redis"
    o = Word2VecCache.get_user_all_news(3333)
    print o, type(o)

    print 'test 3 put empty ids into redis'
    uid = 2
    ids = []
    Word2VecCache.set_user_all_news(uid, ids)
    o = Word2VecCache.get_user_all_news(uid)
    print o, type(o)


if __name__ == '__main__':
    test()
