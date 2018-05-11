# encoding=utf8

from __future__ import absolute_import
from chunyu.utils.general.encoding_utils import ensure_unicode
from rpc_services import rpc_clients
from problem_promotion_service.ttypes import SpecialPopulationParams


def qa_pro_special_population(text, ehr_relation=''):
    params = SpecialPopulationParams(text, ehr_relation)
    return rpc_clients.get_problem_promotion_service_client().special_population_predict(params)


def get_special_population(text):
    text = ensure_unicode(text)
    try:
        return qa_pro_special_population(text)
    except Exception, e:
        print 'get_special_population Exception', e
        return 'common_population'

# print get_special_population("哺乳")
