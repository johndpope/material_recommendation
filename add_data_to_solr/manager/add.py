# encoding=utf8

'''
把物料（科普文章、医生话题等）的信息（tag，tag向量重心（Word2vec），文章类型，话题评分等）
'''

import csv
import json
from random import shuffle

from general_utils.db_utils import get_medicaldb_handler
from add_data_to_solr.cy_solr_local.solr_base import SolrHelper
from general_utils.solr_utils import vec2string
from general_utils.file_utils import pickle_to_file
from general_utils.text_utils import convert2gbk
from rpc_services.word2vec_api import get_vecs2
from general_utils.word2vec_utils import get_center
from rpc_services.medical_service_utils import get_entities,get_entities_cyseg
from add_data_to_solr.manager.add_utils import topic_info, doctor_info, grade_topic

NEWS_INFO_SMALL = {
    "content_type": "news",
    "id_prefix": "news_",
    "doc_vec": "center",
    "clinic_no": "news_type",
    "content": "tags",  # 用特定分隔符连接起来
    "tid": "is_online",
}

NEWS_INFO_BIG = {
    "content_type": "newsbig",
    "id_prefix": "newsbig_",
    "doc_vec": "center",
    "clinic_no": "news_type",
    "content": "tags",
    "tid": "is_online",
}

TOPIC_INFO_SMALL = {
    "content_type": "r_topic",  # 因为表里已经有了content_type=topic，冲突了
    "id_prefix": "r_topic_",
    "doc_vec": "center",
    "title": "title",
    "tid": "topic_score",  # 导入数据时先过滤is_delete=1的话题，这个字段就可以存话题评分啦
    "second_class_clinic_no": "话题的医生对应的科室",
    "content": "tags",
    "content_len":"content_len"
}

TOPIC_INFO_BIG = {
    "content_type": "r_topicbig",
    "id_prefix": "r_topicbig_",
    "doc_vec": "center",
    "title": "title",
    "tid": "topic_score",
    "second_class_clinic_no": "话题的医生对应的科室",
    "content": "tags",
    "content_len":"content_len"
}





def update_tid(docs, solr):
    batch_size = 1000
    batch_num = len(docs) // batch_size + 1 if len(docs) % batch_size != 0 else len(docs) // batch_size
    print "batch_num", batch_num
    for i in range(batch_num):
        tmp_doc = docs[i * batch_size: i * batch_size + batch_size]
        solr.add(tmp_doc, commit=False, softCommit=True, fieldUpdates={"tid": "set"})

        print "% lines added to topic_tpl" % len(tmp_doc)


def add_topic_kernel(topic_id, docs, tags,score, info_of_topic, info_of_doctor, id_prefix, vecs,
                     content_type):
    center = vec2string(get_center(vecs).tolist())
    # score = grade_topic(info_of_topic=info_of_topic, info_of_doctor=info_of_doctor, title_tags=title_tags,
    #                     content_tags=content_tags)
    dic = {
        "id": id_prefix + str(topic_id),
        "content_type": content_type,
        "doc_vec": center,
        "title": info_of_topic["title"],
        "tid":score,
        "second_class_clinic_no": info_of_doctor["second_class_clinic_no"],
        "clinic_no": info_of_doctor["first_class_clinic_no"],
        "content": "|||".join(tags),
        "content_len":info_of_topic["content_len"]
    }
    docs.append(dic)


def add_topic():
    batch_size = 1000
    all_doc_small = []
    all_doc_big = []
    docs_small = []
    docs_big = []
    sql = 'select id from api_doctortopic where is_deleted=0 and title <> "" and id > 154517 limit 20000;'
    o = get_medicaldb_handler().do_one(sql)
    id_prefix_small = "r_topic_"
    id_prefix_big = "r_topicbig_"
    content_type_small = "r_topic"
    content_type_big = "r_topicbig"
    # fo = open("topic_score.csv", "w")
    # csvwriter = csv.writer(fo, dialect='excel')
    # first_line = [u'topic id', u'score', u'topic title', u'content len', u'image num', u'is original',
    #               u'doctor id', u'职称', u'医院级别', u'科室', u'城市']

    # first_line = convert2gbk(first_line)
    # csvwriter.writerow(first_line)
    # index = range(len(o))
    # shuffle(index)
    ff = open("failed_id","a")
    solr = SolrHelper("online").get_solr("topic_tpl")
    is_end = False
    for item in o:
        if item == o[-1]:
            is_end = True
        #print "is_end",is_end
        topic_id = item[0]
        print "topic_id",topic_id
        info_of_topic = topic_info(topic_id)
        topic_title = info_of_topic['title']
        if len(topic_title) == 0:
            #print "empty title",topic_id
            continue
        doctor_id = info_of_topic["doctor_id"]
        info_of_doctor = doctor_info(doctor_id)
        title_tags = get_entities_cyseg(info_of_topic["title"])
        content_tags = get_entities_cyseg(info_of_topic["text"])

        if len(content_tags) == 0:
            print "no content tag",topic_id
            continue

        title_vecs = get_vecs2(title_tags)
        content_vecs = get_vecs2(content_tags)
        print "content_vecs len",len(content_vecs)


        score = int(grade_topic(info_of_topic, info_of_doctor, title_tags, content_tags) * 10)
        if title_vecs and len(title_vecs) > 0:
            #若title有vec，存之
            try:
                add_topic_kernel(topic_id=topic_id,
                                docs=docs_small,
                                 tags=title_tags,
                                 score=score,
                                 info_of_topic=info_of_topic,
                                 info_of_doctor=info_of_doctor,
                                 vecs=title_vecs,
                                 id_prefix=id_prefix_small,
                                 content_type=content_type_small
                                 )
            except:
                ff.write("small|||" + str(topic_id) + "\n")
        if content_vecs and len(content_vecs) > 0:
            #若content有vec，存之
            try:
                add_topic_kernel(topic_id=topic_id,
                                 docs=docs_big,
                                 tags=content_tags,
                                 score=score,
                                 info_of_topic=info_of_topic,
                                 info_of_doctor=info_of_doctor,
                                 vecs=content_vecs,
                                 id_prefix=id_prefix_big,
                                 content_type=content_type_big)
            except:
                ff.write("big|||" + str(topic_id) + "\n")

        ###########




        ############
        print "eln docs_small",len(docs_small)
        print "len docs_big",len(docs_big)
        if len(docs_small) == batch_size or is_end:
            print "topic_id",topic_id
            print "is end",is_end
            print "add small", len(docs_small)

            #print json.dumps(docs_small)
            #add(docs_small,solr)
            all_doc_small.extend(docs_small)
            docs_small = []
        if len(docs_big) == batch_size or is_end:
            print "topic_id", topic_id
            print "is end", is_end
            print "add big", len(docs_big)
            #print json.dumps(docs_big)
            #add(docs_big, solr)
            all_doc_big.extend(docs_big)
            docs_big = []



    ff.close()
    pickle_to_file(all_doc_small,"all_doc_small_3")
    pickle_to_file(all_doc_big,"all_doc_big_3")
            # fo.close()


if __name__ == '__main__':
    add_topic()
