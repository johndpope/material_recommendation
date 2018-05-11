# encoding=utf8


import json
import re
from general_utils.db_utils import get_medicaldb_handler
from general_utils.text_utils import filterHTML
from rpc_services.medical_service_utils import get_entities, get_entities_cyseg

SUPER_CITY = (
    u"北京市",
    u"上海市",
    u"广州市",
    u"深圳市",
)

CAPITAL_CITY = (
    u"济南市",
    u"石家庄市",
    u"长春市",
    u"哈尔滨市",
    u"沈阳市",
    u"呼和浩特市",
    u"乌鲁木齐市",
    u"兰州市",
    u"银川市",
    u"太原市",
    u"西安市",
    u"郑州市",
    u"合肥市",
    u"南京市",
    u"杭州市",
    u"福州市",
    u"南昌市",
    u"海口市",
    u"南宁市",
    u"贵阳市",
    u"长沙市",
    u"武汉市",
    u"成都市",
    u"昆明市",
    u"拉萨市",
    u"西宁市",
    u"天津市",
    u"重庆市",
    u"台北市",
)


def get_text(content_list):
    for item in content_list:
        if item["type"] == "text":
            return item["text"]
    return ""


def topic_info(topic_id):
    sql = "select doctor_id,title,content,html,image,is_original from api_doctortopic where id=%s;" % topic_id
    o = get_medicaldb_handler().do_one(sql)
    if o is None or len(o) == 0:
        return None
    doctor_id = o[0][0]  # unicode
    if not doctor_id:
        return None
    title = o[0][1]  # unicode
    content = o[0][2]  # unicode
    text = ""
    if content and len(content) > 0:
        content = content.replace(u"\r", u"\\r")
        content = content.replace(u"\n", u"\\n")
        content = re.sub(ur"\\+[nrt]", u" ", content)

        content = json.loads(content)
        text = get_text(content)
    elif len(text) == 0:
        html_text = o[0][3]  # unicode
        if html_text:  # default None
            text = filterHTML(html_text)
    content_len = len(text)
    image = o[0][4]
    if image:
        image_num = len(json.loads(image))
    else:
        image_num = 0
    is_original = o[0][5]

    return {
        "doctor_id": doctor_id,
        "title": title,
        "text": text,
        "content_len": content_len,
        "image_num": image_num,
        "is_original": is_original,
    }


def doctor_info(doctor_id):
    sql = "select title,level_title,second_class_clinic_no,first_class_clinic_no,hospital_name from symptomchecker_doctor where id='%s';" % doctor_id
    o = get_medicaldb_handler().do_one(sql)
    if o is None or len(o) == 0:
        return None
    title = o[0][0]  # unicode
    level_title = o[0][1]  # unicode
    second_class_clinic_no = o[0][2]
    first_class_clinic_no = o[0][3]
    hospital_name = o[0][4]
    sql = "select province from clinic_clinicdoctorinfo where doctor_id='%s';" % doctor_id
    o = get_medicaldb_handler().do_one(sql)
    if o is None or len(o) == 0:
        return None
    city = o[0][0]
    return {
        "title": title,
        "hospital_level": level_title,
        "second_class_clinic_no": second_class_clinic_no,
        "first_class_clinic_no": first_class_clinic_no,
        "city": city,
        "hospital_name": hospital_name,
    }


def topic_info_big(topic_id):
    print "calculating", topic_id
    topic_id = int(topic_id)
    info_of_topic = topic_info(topic_id)
    doctor_id = info_of_topic['doctor_id']
    info_of_doctor = doctor_info(doctor_id)
    topic_title = info_of_topic['title']
    topic_content = info_of_topic['text']
    title_tags = get_entities_cyseg(topic_title)

    content_tags = get_entities_cyseg(topic_content)
    score = grade_topic(info_of_topic, info_of_doctor, title_tags, content_tags)

    return score, info_of_topic, info_of_doctor


def grade_topic(info_of_topic, info_of_doc, title_tags, content_tags):
    score = 0.0
    bad_return = 0.0

    # 去掉正文较短，和正文没有实体词的文章
    content_len = info_of_topic["content_len"]
    if content_len <= 10:
        return bad_return

    if len(content_tags) == 0:
        return bad_return

    # 原创加2分，非原创不加分
    if info_of_topic["is_original"] == 1:
        score += 2.0

    # 小于200字0分，200-400字1分，400-600字2分，600-800字3分，800-1000字4分，1000以上5分
    if content_len < 200:
        pass
    elif content_len < 400:
        score += 1.0
    elif content_len < 600:
        score += 2.0
    elif content_len < 800:
        score += 3.0
    elif content_len < 1000:
        score += 4.0
    elif content_len >= 1000:
        score += 5.0

    # 文章标题能提取出实体词，加3分
    # title_tags = get_entities(info_of_topic['title'])
    if len(title_tags) > 0:
        score += 3.0

    # 每张图片加0.5分，最多加2.5分
    image_num = info_of_topic['image_num']
    score += image_num * 0.5 if image_num <= 5 else 2.5

    # 医生北山广深加2分，省会加1分，其他不加分
    city = unicode(info_of_doc['city'])
    if city in SUPER_CITY:
        score += 2.0
    elif city in CAPITAL_CITY:
        score += 1.0

    # 三甲加2分，二甲加1分，其他加0.5
    hostital_level = info_of_doc['hospital_level']
    if hostital_level.startswith(u"三级甲"):
        score += 2.0
    elif hostital_level.startswith(u"二级甲"):
        score += 1.0
    else:
        score += 0.5

    # 主任加3，副主任加2，主治加1
    doc_title = info_of_doc['title']
    if doc_title == u"主任医师":
        score += 3.0
    elif doc_title == u"副主任医师":
        score += 2.0
    elif doc_title == u"主治医师":
        score += 1.0
    else:
        score += 0.5

    return score


def add_all(docs, solr):
    batch_size = 1000
    batch_num = len(docs) // batch_size + 1 if len(docs) % batch_size != 0 else len(docs) // batch_size
    print "batch_num", batch_num
    for i in range(batch_num):
        tmp_doc = docs[i * batch_size: i * batch_size + batch_size]
        solr.add(tmp_doc, commit=False, softCommit=True)

        print "% lines added" % len(tmp_doc)


def update_all(docs, solr, fieldupdate_dict):
    '''
    solr.add(tmp_doc, commit=False, softCommit=True, fieldUpdates={"tid": "set"})
    '''
    batch_size = 1000
    batch_num = len(docs) // batch_size + 1 if len(docs) % batch_size != 0 else len(docs) // batch_size
    print "batch_num", batch_num
    for i in range(batch_num):
        tmp_doc = docs[i * batch_size: i * batch_size + batch_size]
        solr.add(tmp_doc, commit=False, softCommit=True, fieldUpdates=fieldupdate_dict)

        print "% lines updated" % len(tmp_doc)
