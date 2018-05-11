# -*- coding:utf-8 -*-
import json

from medweb_utils.utilities.log_utils import elapsed_logger, info_logger
from rpc_thrift.rpc_wrapper import rpc_wrapper_for_class_method
from material_recommendation_service.MaterialRecommendationService import Iface
from recommend.manager.recommend_resource import Recommend, Recommend_tags, \
    Recommend_topics, Recommend_list, Recommend_plan, Recommend_news


class MaterialRecommendationService(object):
    @classmethod
    def article_recommend(cls, input_list):
        '''
        输入一列用户id，获取他们实时的推荐文章
        :param input: [{"user_id":uid,"timestamp":ts,"resource":"news"},...]
        :return:
        '''
        res = [Recommend(uid=item.get('user_id', -1), lookback=5 * 61.0, end=item.get('timestamp', None),
                         pid=item.get('problem_id', None),
                         ) for item in input_list]

        return {'output': [{'id': item[0], 'title': item[1], 'type': item[2]} for item in res]}

    @classmethod
    def recommend_topn_topic(cls, input_dict):
        '''
        输入一个uid，根据其过去一段时间（前一天）的qa和bs数据，推荐num个医生话题
        :param input_dict:{'user_id':11111,'num':4}
        :return:{'output':[id1,id2,id3,...]
        '''
        uid = input_dict.get('user_id', -1)
        num = input_dict.get('num', 5)
        return {'output': Recommend_topics(uid, num)}

    @classmethod
    def recommend_tags(cls, input_dict):
        '''
        输入一个uid，根据其最后的query，返回若干个相似词 和 解决方案
        :param input_dict: {'user_id':11111}
        :return: {'output':{'words’:[w1,w2,w3,w4,w5,w6],'plan':[{'url':url1,'name':name1},{'url':url2,'name':name2}]}
        '''
        uid = input_dict.get('user_id', -1)
        return {'output': Recommend_tags(uid)}

    @classmethod
    def recommend_list(cls, input_dict):
        '''
        // input_arg: json.dumps({'user_id':11111,'timestamp':17621321,'num':6,'problem_id':222})
        // return: json.dumps({'output':[{'id':111,'type':'topic','title':'xxxx'},{'id':222,'type':'news','title':'yyy'}...])
        string recommend_list(1: string input_arg)   throws (1: RpcException re),
        :param input_dict:
        :return:
        '''
        uid = input_dict.get('user_id', -1)
        timestamp = input_dict.get('timestamp', None)
        num = input_dict.get('num', 6)
        problem_id = input_dict.get('problem_id', None)
        return {'output': Recommend_list(uid=uid, num=num, end=timestamp, pid=problem_id)}

    @classmethod
    def recommend_plan(cls, input_dict):
        '''
        // input_arg: json.dumps({'user_id':11111,'top_n':4})
        // return: json.dumps({'ids':[id1,id2,...]})
        :param input_dict:
        :return:
        '''
        uid = input_dict.get('user_id', -1)
        top_n = input_dict.get('top_n', 4)
        return {'ids': Recommend_plan(uid=uid, num=top_n)}

    @classmethod
    def recommend_news(cls, input_dict):
        '''
        // input_dict: {'user_id': 11111, 'top_n': 2}
        :param input_dict:
        :return:
        '''
        uid = input_dict.get('user_id', -1)
        top_n = input_dict.get('top_n', 2)
        return {'ids': Recommend_news(uid=uid, num=top_n, solr_first=True)}


class MaterialRecommendationProcessor(Iface):
    @rpc_wrapper_for_class_method(elapsed_logger)
    def article_recommend(self, input_arg):
        """
        PARA: string json.dumps([{'user_id':11111,'timestamps':1500000000,'resource_type':'news'}])
        RET: string json.dumps({'output':[{'id':222222,'title':'hhhhhh'}]})
        """
        input_list = json.loads(input_arg)
        return json.dumps(MaterialRecommendationService.article_recommend(input_list))

    @rpc_wrapper_for_class_method(elapsed_logger)
    def recommend_topn_topic(self, input_arg):
        '''
        // input_arg: json.dumps({'user_id':11111,'num':4})
        // return : json.dumps({'output':[id1,id2,id3,...])
        string recommend_topn_topic(1: string input_arg)   throws (1: RpcException re),
        '''
        input_dict = json.loads(input_arg)
        return json.dumps(MaterialRecommendationService.recommend_topn_topic(input_dict))

    @rpc_wrapper_for_class_method(elapsed_logger)
    def recommend_tags(self, input_arg):
        '''
        // input_arg: json.dumps({'user_id':11111})
        // return : json.dumps({'output':{'words’:[w1,w2,w3,w4,w5,w6],'plan':[{'url':url1,'name':name1},{'url':url2,'name':name2}]})
        '''
        input_dict = json.loads(input_arg)
        return json.dumps(MaterialRecommendationService.recommend_tags(input_dict))

    @rpc_wrapper_for_class_method(elapsed_logger)
    def recommend_list(self, input_arg):
        input_dict = json.loads(input_arg)
        return json.dumps(MaterialRecommendationService.recommend_list(input_dict))

    @rpc_wrapper_for_class_method(elapsed_logger)
    def recommend_plan(self, input_arg):
        input_dict = json.loads(input_arg)
        return json.dumps(MaterialRecommendationService.recommend_plan(input_dict))

    @rpc_wrapper_for_class_method(elapsed_logger)
    def recommend_news(self, input_arg):
        input_dict = json.loads(input_arg)
        return json.dumps(MaterialRecommendationService.recommend_news(input_dict))
