# encoding=utf8

import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
from random import choice

from chunyu.utils.general.encoding_utils import ensure_unicode

from recommend.app_config import RESOURCE_DATA_FILE, TOPIC_SCORE_FILE, BODYPART_FILE, MEDICAL_ENTITY_FILE, \
    SYSTAG_DATA_FILE, MEDICAL_RELATION_DRUG_FILE
from recommend.consts import CATE_MAP
from global_config import get_root_path
from general_utils import simple_mysql
from settings_common.settings_dbs import DATABASE_ONLINE_MEDICAL as DATABASE_MEDICAL
from settings_common.settings_dbs import DATABASE_NEWS, DATABASES_BIGDATA_NEW, DATABASES_BIGDATA_OFFLINE, \
    DATABASE_DIAGNOSE
from general_utils.time_utils import timestamp2datetime
from general_utils.file_utils import pickle_from_file, load_simple_lines

_medicaldb_handler = None
_newsdb_handler = None
_medical_entity_handler = None
_medical_entity_handler_test = None
_diagnose_handler = None
_db_data_local_handler = None

STOP_WORD_PATH = os.path.join(get_root_path(), 'general_utils/data_dir/stop_words.txt')


def parse_dbconfig(settings_dbconfig):
    simple_dbconfig = {}
    simple_dbconfig['host'] = settings_dbconfig['default']['HOST']
    simple_dbconfig['db'] = settings_dbconfig['default']['NAME']
    simple_dbconfig['user'] = settings_dbconfig['default']['USER']
    simple_dbconfig['passwd'] = settings_dbconfig['default']['PASSWORD']
    try:
        simple_dbconfig['port'] = int(settings_dbconfig['default']['PORT'])
    except:
        simple_dbconfig['port'] = ''
    simple_dbconfig['charset'] = settings_dbconfig['default']['OPTIONS']['charset']
    return simple_dbconfig


class MedicaldbHandler(object):
    def __init__(self):
        self.dbhandler = simple_mysql.DoMany(parse_dbconfig(DATABASE_MEDICAL))

    def do_one(self, sql, commit=False):
        return self.dbhandler.do_one(sql, commit)

    def get_ask(self, uid, begin, end):
        # 获取用户一段时间内的qa首问，begin,end都是时间戳，需要转换成datetime
        begin = timestamp2datetime(begin)
        end = timestamp2datetime(end)
        sql = 'select ask from ask_problem where user_id=%s and created_time > "%s" and created_time < "%s";' % (
            uid, begin, end)
        print "get_ask sql", sql
        o = self.dbhandler.do_one(sql)
        all_ask = []
        if o is None:
            return all_ask
        for item in o:
            ask = item[0]  # unicode
            all_ask.append(ask)
        return all_ask

    def get_ask_by_timestamp(self, uid, timestamp):
        if isinstance(timestamp, int):
            timestamp /= 1000.0
        datetime = timestamp2datetime(timestamp)
        sql = 'select id,ask from ask_problem where user_id=%s and created_time="%s";' % (uid, datetime)

        o = self.dbhandler.do_one(sql)
        if o is None or len(o) == 0:
            return None, None
        return o[0][0], o[0][1]

    def get_ask_by_pid(self, pid):
        sql = 'select ask from ask_problem where id=%s;' % pid
        o = self.dbhandler.do_one(sql)
        if o is None or len(o) == 0:
            return ''
        return o[0][0]

    def get_topic_hospital(self, topic_id):
        sql1 = "select doctor_id from api_doctortopic where id=%s;" % topic_id
        o1 = self.do_one(sql1)
        if o1 is None or len(o1) == 0:
            return ''
        doctor_id = o1[0][0]
        sql2 = "select hospital_name from symptomchecker_doctor where id='%s';" % doctor_id
        o2 = self.do_one(sql2)
        if o2 is None or len(o2) == 0:
            return ''
        return o2[0][0]

    def get_topic_clinic_no(self, topic_id):
        bad_return = None, None

        sql1 = "select doctor_id from api_doctortopic where id=%s;" % topic_id
        o1 = self.do_one(sql1)
        if o1 is None or len(o1) == 0:
            return bad_return

        doctor_id = o1[0][0]
        sql2 = "select first_class_clinic_no, second_class_clinic_no from symptomchecker_doctor where id='%s';" % doctor_id
        o2 = self.do_one(sql2)
        if o2 is None or len(o2) == 0:
            return bad_return
        return o2[0][0], o2[0][1]

    def get_topic_title(self, topic_id):
        bad_return = ""
        sql = "select title from api_doctortopic where id=%s;" % topic_id
        o = self.do_one(sql)
        if o is None or len(o) == 0:
            return bad_return
        return o[0][0]

    def get_topic_doctor_id(self, id):
        sql = 'select doctor_id from api_doctortopic where id=%s;' % id
        o = self.dbhandler.do_one(sql)
        if o is None or len(o) == 0:
            return None
        return o[0][0]


class NewsdbHandler(object):
    def __init__(self):
        self.dbhandler = simple_mysql.DoMany(parse_dbconfig(DATABASE_NEWS))

    def get_all_articles(self, start=0, keep_content=False):
        sql = 'select id,title,digest,content,news_type from news_healthnews where id>=%s;' % start
        o = self.dbhandler.do_one(sql)
        if (o is None) or (not o):
            return None
        output = {}
        for item in o:
            id = int(item[0])
            title = item[1]  # unicode
            digest = item[2]  # unicode
            content = item[3]
            news_type = item[4]
            if keep_content:
                output[id] = [title, digest, content, news_type]
            else:
                output[id] = [title, digest, u'', news_type]

        return output

    def get_all_articles_simple(self):
        sql = 'select id,title,digest from news_healthnews;'
        o = self.dbhandler.do_one(sql)
        if (o is None) or (not o):
            return None
        output = {}
        for item in o:
            id = int(item[0])
            title = item[1]  # unicode
            digest = item[2]  # unicode

            output[id] = [title, digest]
        return output

    def do_one(self, sql, commit=False):
        return self.dbhandler.do_one(sql, commit)

    def get_title_digest_by_nid(self, news_id):
        sql = 'select title,digest from news_healthnews where id=%s;' % news_id
        o = self.dbhandler.do_one(sql)
        if (o is None) or (not o):
            return '', ''
        return o[0][0], o[0][1]

    def get_title_digest_by_nids(self, news_ids):
        return [self.get_title_digest_by_nid(id) for id in news_ids]

    def get_content_by_nid(self, news_id):
        sql = 'select content from news_healthnews where id=%s;' % news_id
        o = self.dbhandler.do_one(sql)
        if (o is None) or (not o):
            return ''
        return o[0][0]

    def get_content_by_nids(self, news_ids):
        return [self.get_content_by_nid(x) for x in news_ids]

    def news_type(self, news_id):
        sql = 'select news_type from news_healthnews where id=%s;' % news_id
        o = self.dbhandler.do_one(sql)
        if o is None or len(o) == 0:
            return None
        return o[0][0]


class MedicalEntitiydbHandler(object):
    def __init__(self, test=False):
        if not test:
            self.dbhandler = simple_mysql.DoMany(parse_dbconfig(DATABASES_BIGDATA_NEW))
        else:
            self.dbhandler = simple_mysql.DoMany(parse_dbconfig(DATABASES_BIGDATA_OFFLINE))

    def get_all_relation_drug(self):
        # 获取所有疾病和症状词的相关药品词
        drug_relations = {}
        for cate in ('diseases', 'symptoms'):
            sql = 'select name,relation_drug from medicaldb_new%s where relation_drug<>"";' % cate
            o = self.dbhandler.do_one(sql)
            for item in o:
                name = item[0]  # unicode
                relation_drug = item[1]  # unicode
                if len(relation_drug.strip()) == 0:  # 没有相关药品
                    continue
                relation_drugs = relation_drug.split('|')
                for drug in relation_drugs:
                    if len(drug) == 0:
                        continue
                    if drug not in drug_relations:
                        drug_relations[drug] = set([name])
                    else:
                        drug_relations[drug].add(name)
        return drug_relations

    def do_one(self, sql, commit=False):
        return self.dbhandler.do_one(sql, commit)

    def get_all_diseases_tags(self):
        disease = set()
        sql = 'select name from medicaldb_newdiseases;'
        o = self.dbhandler.do_one(sql)
        for item in o:
            tag = item[0]
            disease.add(tag)
        return disease


class DiagnoseHandler(object):
    def __init__(self):
        self.dbhandler = simple_mysql.DoMany(parse_dbconfig(DATABASE_DIAGNOSE))

    def test(self, sysTag_id):
        sql = 'select keywords from ner_systagsolrgenerateconf where sysTag_id=%s;' % sysTag_id
        o = self.dbhandler.do_one(sql)
        print o

    def get_systag_en_name(self, sysTag_id):
        sql = 'select name from ner_systagconf where id=%s;' % sysTag_id
        o = self.dbhandler.do_one(sql)
        if (o is None) or (not o):
            return ''
        return o[0][0]


class DbDataLocalHandler(object):
    # data = pickle_from_file(RESOURCE_DATA_FILE)
    topic_score = pickle_from_file(TOPIC_SCORE_FILE)
    # stop_word = load_simple_lines(STOP_WORD_PATH)
    bodypart_data = pickle_from_file(BODYPART_FILE)
    medical_entity_cate = pickle_from_file(MEDICAL_ENTITY_FILE)
    medical_relation_drug = pickle_from_file(MEDICAL_RELATION_DRUG_FILE)
    systag_data = pickle_from_file(SYSTAG_DATA_FILE)

    @classmethod
    def get_news_title(cls, news_id):
        news_id = int(news_id)
        if news_id in cls.data["news"]:
            return cls.data["news"][news_id][0]
        else:
            return u''

    @classmethod
    def get_news_digest(cls, news_id):
        news_id = int(news_id)
        if news_id in cls.data["news"]:
            return cls.data["news"][news_id][1]
        else:
            return u''

    @classmethod
    def get_news_content(cls, news_id):
        news_id = int(news_id)
        if news_id in cls.data["news"]:
            return cls.data["news"][news_id][2]
        return u''

    @classmethod
    def get_news_type(cls, news_id):
        news_id = int(news_id)
        if news_id in cls.data["news"]:
            return cls.data["news"][news_id][3]
        return u''

    @classmethod
    def get_topic_score(cls, topic_id):
        topic_id = int(topic_id)
        if topic_id in cls.topic_score:
            return cls.topic_score[topic_id]
        return 0.0

    @classmethod
    def is_in_bodypart(cls, word):
        word = ensure_unicode(word)
        if word == u"血":
            return False
        return word in cls.bodypart_data

    @classmethod
    def get_entity_cate(cls, word):
        word = ensure_unicode(word.lower())
        return cls.medical_entity_cate.get(word, '')

    @classmethod
    def is_entity(cls, word):
        word = ensure_unicode(word)
        return word in cls.medical_entity_cate

    @classmethod
    def get_relation_drug(cls, word, num=100):
        word = ensure_unicode(word)
        return cls.medical_relation_drug.get(word, [])[:num]

    @classmethod
    def get_keyword_relation_systag_id(cls, word):
        word = ensure_unicode(word)
        return cls.systag_data['keyword'].get(word, [])
        # return choice([[1, 2], [4]])

    @classmethod
    def get_systag_relation_plan(cls, systag_id):
        # systag_id = unicode(systag_id)
        if systag_id not in cls.systag_data['systag']:
            return []
        return cls.systag_data['systag'][systag_id]['plan']

    @classmethod
    def get_systagid_name(cls, systag_id):
        return cls.systag_data['systag'][systag_id]['tag_name']

    @classmethod
    def get_systagid_relation_planid(cls, systag_id):
        systag_id = int(systag_id)
        try:
            return [item['plan_id'] for item in cls.systag_data['systag'][systag_id]['plan']]
        except:
            return []

    @classmethod
    def get_extend_keyword_relation_systag_id(cls, word):
        # 对热卖tag进行相似词扩展后
        word = ensure_unicode(word)
        # return [systag_id_list,similarity]
        return cls.systag_data['keyword_extend'].get(word, [[], 0.0])

    @classmethod
    def clinic_no_relation_systag_id(cls, clinic_no):
        clinic_no = unicode(clinic_no)
        return cls.systag_data['clinic_no'].get(clinic_no, [])


def get_db_data_local_handler():
    global _db_data_local_handler
    if not _db_data_local_handler:
        _db_data_local_handler = DbDataLocalHandler()
    return _db_data_local_handler


def get_medicaldb_handler():
    global _medicaldb_handler
    if not _medicaldb_handler:
        _medicaldb_handler = MedicaldbHandler()
    return _medicaldb_handler


def get_newsdb_handler():
    global _newsdb_handler
    if not _newsdb_handler:
        _newsdb_handler = NewsdbHandler()
    return _newsdb_handler


def get_medical_entity_handler(test):
    global _medical_entity_handler
    if not _medical_entity_handler:
        _medical_entity_handler = MedicalEntitiydbHandler(test)
    return _medical_entity_handler


def get_diagnose_handler():
    global _diagnose_handler
    if not _diagnose_handler:
        _diagnose_handler = DiagnoseHandler()
    return _diagnose_handler


##################################################################

ABBR_SPLITER = u'|'
ALIAS_SPLITER = u'|'


def get_drug_data(entity_info_dict, is_test):
    # 适用于medicaldb_newdrugs
    table_name = "medicaldb_newdrugs"
    if is_test:
        sql = "select id, cn_name, common_name from %s;" % table_name
    else:
        sql = "select id, cn_name, common_name, abbr, alias from %s;" % table_name
    o = get_medical_entity_handler(is_test).do_one(sql)
    uniform_cate = CATE_MAP.get(table_name, "")
    for item in o:
        word_id = unicode(item[0])
        cn_name = item[1].replace(" ", "")
        common_name = item[2].replace(" ", "")
        name_list = [cn_name, common_name]
        if not is_test:
            abbr = item[3].replace(" ", "").split(ABBR_SPLITER)
            alias = item[4].replace(" ", "").split(ALIAS_SPLITER)
            name_list.extend(abbr + alias)
        for x in name_list:
            entity_info_dict[x] = {"cate": uniform_cate, "id": word_id}


def get_checkup_data(entity_info_dict, is_test, table_name):
    # 适用于medicaldb_newoperations, medicaldb_newcheckups 两表
    if is_test:
        sql = "select id, name from %s;" % table_name
    else:
        sql = "select id, name, abbr, alias from %s;" % table_name
    o = get_medical_entity_handler(is_test).do_one(sql)
    uniform_cate = CATE_MAP.get(table_name, "")

    for item in o:
        word_id = unicode(item[0])
        name = item[1].replace(" ", "")
        name_list = [name]
        # entity_info_dict[name] = {"cate": uniform_cate, "id": word_id}
        if not is_test:
            abbr = item[2].replace(" ", "").split(ABBR_SPLITER)
            alias = item[3].replace(" ", "").split(ALIAS_SPLITER)
            name_list.extend(abbr + alias)

        for x in name_list:
            entity_info_dict[x] = {"cate": uniform_cate, "id": word_id}


def get_bodypart_data(entity_info_dict, is_test, table_name):
    # medicaldb_bodypart, medicaldb_clinic
    if is_test:
        sql = "select id, name from %s;" % table_name
    else:
        sql = "select id, name, abbr from %s;" % table_name
    o = get_medical_entity_handler(is_test).do_one(sql)
    uniform_cate = CATE_MAP.get(table_name, "")
    for item in o:
        word_id = unicode(item[0])
        name = item[1].replace(" ", "")
        name_list = [name]
        if not is_test:
            abbr = item[2].replace(" ", "").split(ABBR_SPLITER)
            name_list.extend(abbr)
        for x in name_list:
            entity_info_dict[x] = {"cate": uniform_cate, "id": word_id}


def get_symptom_data(entity_info_dict, is_test, table_name):
    # 适用于medicaldb_newsymptoms, medicaldb_newdiseases表
    if is_test:
        sql = "select id, name, relation_drug, frequency from %s;" % table_name
    else:
        sql = "select id, name, relation_drug, frequency, abbr, alias from %s;" % table_name
    o = get_medical_entity_handler(is_test).do_one(sql)
    uniform_cate = CATE_MAP.get(table_name, "")
    for item in o:
        word_id = unicode(item[0])
        name = item[1].replace(" ", "")
        relation_drug = item[2]
        freq = item[3]

        name_list = [name]
        if not is_test:
            abbr = item[4].replace(" ", "").split(ABBR_SPLITER)
            alias = item[5].replace(" ", "").split(ALIAS_SPLITER)
            name_list.extend(abbr + alias)

        if not relation_drug:
            relation_drug = ''
        relation_drug = filter(lambda y: len(y) > 0, [x.strip() for x in relation_drug.split('|')])

        for x in name_list:
            if x in entity_info_dict:
                new_relation_drug = relation_drug if len(relation_drug) > 0 else entity_info_dict[x].get(
                    "relation_drug",
                    [])
            else:
                new_relation_drug = relation_drug
            entity_info_dict[x] = {"freq": freq,
                                   "cate": uniform_cate,
                                   "id": word_id,
                                   "relation_drug": new_relation_drug}


def get_entity_cate():
    entity_info_dict = {}

    get_drug_data(entity_info_dict, False)

    get_checkup_data(entity_info_dict, False, "medicaldb_newcheckups")
    get_checkup_data(entity_info_dict, False, "medicaldb_newoperations")

    get_bodypart_data(entity_info_dict, False, "medicaldb_bodypart")
    get_bodypart_data(entity_info_dict, False, "medicaldb_clinic")

    get_symptom_data(entity_info_dict, False, "medicaldb_newsymptoms")
    get_symptom_data(entity_info_dict, False, "medicaldb_newdiseases")

    entity_cate = {}
    entity_relation_drug = {}

    if u"" in entity_info_dict:
        del entity_info_dict[u""]
    if u" " in entity_info_dict:
        del entity_info_dict[u" "]

    for entity in entity_info_dict:
        cate = entity_info_dict[entity].get('cate', '')
        relation_drug = entity_info_dict[entity].get('relation_drug', [])

        entity_set = set([entity, entity.lower()])
        if relation_drug:
            for e in entity_set:
                entity_relation_drug[e] = relation_drug
        if cate:
            for e in entity_set:
                entity_cate[e] = cate
    return entity_cate, entity_relation_drug


# +++++++++++++##+++++++++++++++++++++++
def test_DiagnoseHandler():
    h = get_db_data_local_handler()
    words = ["恐惧", "害怕", "唐氏筛查", "", None]
    for x in words:
        print x
        print h.get_keyword_relation_systag_id(x)

    print h.get_systag_relation_plan(114)


def test():
    import sys
    clinic_no = sys.argv[1]

    o = get_db_data_local_handler().get_systagid_relation_planid(clinic_no)
    print o


if __name__ == '__main__':
    test()
# test_DiagnoseHandler()
