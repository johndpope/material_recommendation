# encoding=utf8

import sys
import time
from general_utils.db_utils import get_medical_entity_handler
from add_data_to_solr.manager.add_utils import add_all
from add_data_to_solr.cy_solr_local.solr_base import SolrHelper, SolrCloud, ZooKeeper

'''
bodypart
clinic
disease
symptom
drug
operation
checkup
'''


def add_bdpart():
    '''
    fields:
     id : "bodypart_1231231"
     name : ["眼睛"]
     type: "bodypart"
    '''

    sql = 'select id,name,abbr from medicaldb_bodypart;'
    id_prefix = "bodypart_"
    o = get_medical_entity_handler(False).do_one(sql)

    docs = []

    for item in o:
        id = item[0]
        name = item[1].lower().replace(" ", "")
        abbr = item[2].lower().replace(" ", "")
        name_list = [name, abbr] if abbr else [name]
        docs.append(
            {
                "id": id_prefix + str(id),
                "name": name_list,
                "name_string":name_list,
                "type": "bodypart",
                "timestamp": int(time.time() * 1000)
            }
        )

    solr = SolrCloud(ZooKeeper("md4:2181"), "simple_medical_entity")
    add_all(docs, solr)


def add_clinic():
    # medicaldb_clinic
    sql = 'select id,name,abbr from medicaldb_clinic;'
    id_prefix = "clinic_"
    o = get_medical_entity_handler(False).do_one(sql)

    docs = []

    for item in o:
        id = item[0]
        name = item[1].lower().replace(" ", "")
        abbr = item[2].lower().replace(" ", "")
        name_list = [name, abbr] if abbr else [name]
        docs.append(
            {
                "id": id_prefix + str(id),
                "name": name_list,
                "name_string": name_list,
                "type": "clinic",
                "timestamp": int(time.time() * 1000)
            }
        )

    solr = SolrCloud(ZooKeeper("md4:2181"), "simple_medical_entity")
    add_all(docs, solr)


def add_operation():
    # medicaldb_newoperations
    sql = 'select id,name,abbr,alias from medicaldb_newoperations;'
    id_prefix = "operation_"
    o = get_medical_entity_handler(False).do_one(sql)

    docs = []

    for item in o:
        id = item[0]
        name = item[1].lower().replace(" ", "")
        abbr = item[2].lower().replace(" ", "")
        alias = item[3].lower().replace(" ", "")
        name_list = [name]
        if abbr:
            name_list.append(abbr)
        if alias:
            alias_list = alias.split('|')
            name_list.extend(alias_list)

        name_set = set(name_list)
        name_set.discard("")
        name_list = list(name_set)

        docs.append(
            {
                "id": id_prefix + str(id),
                "name": name_list,
                "name_string": name_list,
                "type": "operation",
                "timestamp": int(time.time() * 1000)
            }
        )

    solr = SolrCloud(ZooKeeper("md4:2181"), "simple_medical_entity")
    add_all(docs, solr)


def add_checkup():
    # medicaldb_newcheckups
    sql = 'select id,name,abbr,alias from medicaldb_newcheckups;'
    id_prefix = "checkup_"
    o = get_medical_entity_handler(False).do_one(sql)

    docs = []

    for item in o:
        id = item[0]
        name = item[1].lower().replace(" ", "")
        abbr = item[2].lower().replace(" ", "")
        alias = item[3].lower().replace(" ", "")
        name_list = [name]
        if abbr:
            name_list.append(abbr)
        if alias:
            alias_list = alias.split('|')
            name_list.extend(alias_list)

        name_set = set(name_list)
        name_set.discard("")
        name_list = list(name_set)

        docs.append(
            {
                "id": id_prefix + str(id),
                "name": name_list,
                "name_string": name_list,
                "type": "checkup",
                "timestamp": int(time.time() * 1000)
            }
        )

    solr = SolrCloud(ZooKeeper("md4:2181"), "simple_medical_entity")
    add_all(docs, solr)


def add_drug():
    # medicaldb_newdrugs
    sql = "select id,cn_name,common_name,abbr,alias from medicaldb_newdrugs;"
    id_prefix = "drug_"
    o = get_medical_entity_handler(False).do_one(sql)
    docs = []

    for item in o:
        id = item[0]
        cn_name = item[1].lower().replace(" ", "")
        common_name = item[2].lower().replace(" ", "")
        abbr = item[3].lower().replace(" ", "")
        alias = item[4].lower().replace(" ", "")

        name_list = []
        if cn_name:
            name_list.append(cn_name)
        if common_name:
            name_list.append(common_name)
        if abbr:
            name_list.append(abbr)
        if alias:
            alias_list = alias.split('|')
            name_list.extend(alias_list)
        if not name_list:
            continue

        name_set = set(name_list)
        name_set.discard("")
        name_list = list(name_set)

        docs.append(
            {
                "id": id_prefix + str(id),
                "name": name_list,
                "name_string": name_list,
                "type": "drug",
                "timestamp": int(time.time() * 1000)
            }
        )
    solr = SolrCloud(ZooKeeper("md4:2181"), "simple_medical_entity")
    add_all(docs, solr)


def add_disease():
    # medicaldb_newdiseases
    sql = "select id,name,abbr,alias from medicaldb_newdiseases;"
    id_prefix = "disease_"
    o = get_medical_entity_handler(False).do_one(sql)
    docs = []

    for item in o:
        id = item[0]
        name = item[1].lower().replace(" ", "")
        abbr = item[2].lower().replace(" ", "")
        alias = item[3].lower().replace(" ", "")
        name_list = [name]
        if abbr:
            name_list.append(abbr)
        if alias:
            alias_list = alias.split('|')
            name_list.extend(alias_list)

        name_set = set(name_list)
        name_set.discard("")
        name_list = list(name_set)

        docs.append(
            {
                "id": id_prefix + str(id),
                "name": name_list,
                "name_string": name_list,
                "type": "disease",
                "timestamp": int(time.time() * 1000)
            }
        )
    solr = SolrCloud(ZooKeeper("md4:2181"), "simple_medical_entity")
    add_all(docs, solr)


def add_symptom():
    # medicaldb_newsymptoms
    sql = "select id,name,abbr,alias from medicaldb_newsymptoms;"
    id_prefix = "symptom_"
    o = get_medical_entity_handler(False).do_one(sql)
    docs = []

    for item in o:
        id = item[0]
        name = item[1].lower().replace(" ", "")
        abbr = item[2].lower().replace(" ", "")
        alias = item[3].lower().replace(" ", "")
        name_list = [name]
        if abbr:
            name_list.append(abbr)
        if alias:
            alias_list = alias.split('|')
            name_list.extend(alias_list)

        name_set = set(name_list)
        name_set.discard("")
        name_list = list(name_set)

        docs.append(
            {
                "id": id_prefix + str(id),
                "name": name_list,
                "name_string": name_list,
                "type": "symptom",
                "timestamp": int(time.time() * 1000)
            }
        )
    solr = SolrCloud(ZooKeeper("md4:2181"), "simple_medical_entity")
    add_all(docs, solr)


if __name__ == '__main__':
    add_bdpart()
    add_clinic()
    add_checkup()
    add_operation()
    add_drug()
    add_disease()
    add_symptom()
