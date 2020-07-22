# encoding=utf8

from collections import defaultdict
import json

from django.core.management.base import BaseCommand
from chunyu.utils.general.encoding_utils import ensure_unicode

from recommend.app_config import RESOURCE_DATA_FILE, TOPIC_DATA_FILE, TOPIC_SCORE_FILE, BODYPART_FILE, \
    MEDICAL_ENTITY_FILE, SYSTAG_DATA_FILE, SYSTAG_DATA_CHECK_FILE, MEDICAL_RELATION_DRUG_FILE, get_high_freq_words
from general_utils.file_utils import pickle_to_file, pickle_from_file
from rpc_services.medical_service_utils import get_entities, get_entities_cyseg
from rpc_services.word2vec_api import get_similar_redis

pickle_to_file({}, RESOURCE_DATA_FILE)

from general_utils.db_utils import get_newsdb_handler, get_medicaldb_handler, get_medical_entity_handler, \
    get_entity_cate, get_diagnose_handler
from add_data_to_solr.manager.add_utils import topic_info, doctor_info, grade_topic


class Command(BaseCommand):
    def handle(self, *args, **options):
        """
        ./PYTHON.sh manage.py get_local_data
        """

        # get_and_save_resource()
        # get_topic_data()
        get_bodypart_data()
        get_simple_medical_entity_data()
        get_systag_data()


def get_and_save_resource():
    resource_data = {}
    resource_data["news"] = get_newsdb_handler().get_all_articles()
    for x in resource_data:
        print "resource_data", x, len(resource_data[x])
    pickle_to_file(resource_data, RESOURCE_DATA_FILE)


def get_topic_data():
    # score
    old_score = pickle_from_file(TOPIC_SCORE_FILE)
    biggest_id = max(old_score.keys())  # 最大的topic_id
    sql1 = "select id,doctor_id from api_doctortopic where is_deleted=0 and title <> '' and id>%s;" % biggest_id
    o = get_medicaldb_handler().do_one(sql1)
    cnt = 0
    for item in o:
        id = item[0]
        doctor_id = item[1]
        info_of_topic = topic_info(id)
        info_of_doc = doctor_info(doctor_id)
        title_tags = get_entities_cyseg(info_of_topic["title"])

        content_tags = get_entities_cyseg(info_of_topic["text"])
        # print "content",info_of_topic["text"]

        if len(content_tags) == 0 or len(info_of_topic['title']) == 0:
            print "no content tag", id
            continue

        score = grade_topic(info_of_topic, info_of_doc, title_tags, content_tags)
        old_score[int(id)] = score
        cnt += 1
    print "new topic id num", cnt
    pickle_to_file(old_score, TOPIC_SCORE_FILE)


def get_bodypart_data():
    all_bodypart = set()
    sql = 'select name from medicaldb_bodypart;'
    o = get_medical_entity_handler(False).do_one(sql)
    for item in o:
        name = item[0]
        all_bodypart.add(name)
    print "all_bodypart len is", len(all_bodypart)
    pickle_to_file(all_bodypart, BODYPART_FILE)


def get_simple_medical_entity_data():
    # 获取实体词极其category的对应字典
    entity_cate, entity_relation_drug = get_entity_cate()
    pickle_to_file(entity_cate, MEDICAL_ENTITY_FILE)
    pickle_to_file(entity_relation_drug, MEDICAL_RELATION_DRUG_FILE)


def get_systag_data():
    # 获取热卖tag相关数据，keywords,target_param,name等
    sql = "select sysTag_id, keywords ,clinic_no,second_clinic_no from ner_systagsolrgenerateconf;"
    data = dict()
    data[
        "systag"] = {}
    # 9:{'tag_name':'gastroscope_colonoscope','plan':[{'url':url1,'name':name1},{'url':url2,'name':name2}]}
    data['keyword'] = defaultdict(list)  # '感冒':[systag_id1,systag_id2...]
    data['keyword_extend'] = {}
    data['clinic_no'] = defaultdict(list)  # u'1':[systag_id1]
    all_plan_name = []
    o = get_diagnose_handler().dbhandler.do_one(sql)

    for item in o:
        systag_id = item[0]
        keywords = item[1].strip()
        clinic_no = item[2].strip()
        second_clinic_no = item[3].strip()

        # 科室信息与systag_id的对应关系，不标记区分一二级科室
        if clinic_no:
            clinic_nos = clinic_no.split()
            for x in clinic_nos:
                x = ensure_unicode(x)
                data['clinic_no'][x].append(systag_id)
        if second_clinic_no:
            second_clinic_nos = second_clinic_no.split()
            for x in second_clinic_nos:
                x = ensure_unicode(x)
                data['clinic_no'][x].append(systag_id)

        # data['systag']
        tag_name = get_diagnose_handler().get_systag_en_name(systag_id)
        sql1 = 'select id,name,target_param from api_userhomehotsalegallery where tag="%s" and is_online=1;' % tag_name
        o1 = get_medicaldb_handler().do_one(sql1)

        data['systag'][systag_id] = {'tag_name': tag_name,
                                     'plan': []}

        if not o1:
            continue

        for item1 in o1:
            plan_id = item1[0]
            name = item1[1]
            url = item1[2].replace('\r\n', '')
            print systag_id, tag_name, name, url
            data['systag'][systag_id]['plan'].append({'url': url, 'name': name, 'plan_id': plan_id})

            all_plan_name.append([systag_id, name])

        if keywords == u"*":
            continue
            # data['keyword']
        keywords = keywords.lower().split()
        for k in keywords:
            if systag_id not in data['keyword'][k]:
                data['keyword'][k].append(systag_id)

    # 用相似词将keyword扩充
    num = 20
    main_subordinate = {}
    high_freq_words = get_high_freq_words()

    for k in data['keyword']:
        systag_id_list = data['keyword'][k]
        # data['keyword_extend'][k] = [systag_id_list, 1.0]
        main_subordinate[k] = [systag_id_list, []]
        for w, s in get_similar_redis(k, num):
            w = ensure_unicode(w)
            if len(w) < 2:
                # 去掉长度为1的相似词
                continue
            if s < 0.41:
                # 分数过低的不要
                break
            if w in high_freq_words:
                # 去掉公认的高频词
                continue

            data['keyword_extend'][w] = [systag_id_list, s]
            main_subordinate[k][1].append([w, s])

    for k in data['keyword']:
        systag_id_list = data['keyword'][k]
        data['keyword_extend'][k] = [systag_id_list, 1.0]

    # 把keyword_extend信息存文件里，方便查看
    with open(SYSTAG_DATA_CHECK_FILE, 'w') as fc:
        for k in main_subordinate:
            systag_id_list, ws_list = main_subordinate[k]
            fc.write('###' + k + '|||' + json.dumps(systag_id_list) + '=' * 10 + '\n')
            for w, s in ws_list:
                fc.write(w + '|||' + str(s) + '\n')
        for systag_id, plan_name in all_plan_name:
            fc.write(str(systag_id) + '---' + plan_name + '\n')

    pickle_to_file(data, SYSTAG_DATA_FILE)
