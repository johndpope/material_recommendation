# encoding=utf8
from __future__ import absolute_import
from rpc_services import rpc_clients
from problem_triage_service.ttypes import TriageParams
from recommend.consts import CLINIC_NO_MAP


def second_clinic(query, has_image=None, problem_id=None):
    result = rpc_clients.get_problem_triage_client().triage_second(
        TriageParams(content=query, has_image=has_image, problem_id=problem_id))
    return result.clinic_ids[0]


def first_clinic(query):
    second_class_clinic_no = second_clinic(query)
    return CLINIC_NO_MAP.get(second_class_clinic_no, second_class_clinic_no)


def test():
    text = ' '
    o = first_clinic(text)
    print o
    print type(o)

# test()


