# encoding=utf8
ALL_CLINIC_CHOICES = [
    ('', -1, u'全部'),
    ('0', 1, u'春雨综合科'),
    ('1', 2, u'妇科'),
    ('2', 3, u'儿科'),
    ('3', 4, u'内科'),
    ('4', 5, u'皮肤性病科'),
    ('6', 6, u'营养科'),
    ('7', 7, u'骨伤科'),
    ('8', 8, u'男科'),
    ('9', 9, u'外科'),
    ('11', 10, u'肿瘤及防治科'),
    ('12', 11, u'中医科'),
    ('13', 12, u'口腔颌面科'),
    ('14', 13, u'耳鼻咽喉科'),
    ('15', 14, u'眼科'),
    ('16', 15, u'整形美容科'),
    ('17', 16, u'精神心理科'),
    ('21', 17, u'产科'),
    ('19', 18, u'基因检测科'),
    ('22', 19, u'报告解读科'),
    ('111', 20, u'春雨全科'),
    ('222', 21, u'夜间全科'),
    ('333', 22, u'非医疗健康问题'),
    ('aa', 23, u'呼吸内科'),
    ('ab', 24, u'心血管内科'),
    ('ac', 25, u'神经内科'),
    ('ad', 26, u'消化内科'),
    ('ae', 27, u'肾内科'),
    ('af', 28, u'内分泌与代谢科'),
    ('ag', 29, u'风湿免疫科'),
    ('ah', 30, u'血液病科'),
    ('ai', 31, u'感染科'),
    ('aj', 32, u'基层全科'),
    ('ba', 33, u'胸外科'),
    ('bb', 34, u'心脏与血管外科'),
    ('bc', 35, u'神经外科'),
    ('bd', 36, u'肝胆外科'),
    ('be', 37, u'烧伤科'),
    ('bf', 38, u'康复科'),
    ('bg', 39, u'泌尿外科'),
    ('bh', 40, u'肛肠科'),
    ('bi', 41, u'普外科'),
    ('bj', 42, u'甲状腺乳腺外科'),
    ('bk', 43, u'基层全科'),
    ('ca', 44, u'脊柱科'),
    ('cb', 45, u'关节科'),
    ('cc', 46, u'创伤科'),
    ('cd', 47, u'基层全科'),
    ('fa', 48, u'小儿科'),
    ('fb', 49, u'新生儿科'),
    ('fc', 50, u'基层全科'),
    ('ha', 51, u'皮肤科'),
    ('hb', 52, u'性病科'),
    ('hc', 53, u'基层全科'),
    ('ja', 54, u'耳科'),
    ('jb', 55, u'鼻科'),
    ('jc', 56, u'咽喉科'),
    ('jd', 57, u'基层全科'),
    ('ma', 58, u'肿瘤内科'),
    ('mb', 59, u'肿瘤外科'),
    ('mc', 60, u'介入与放疗中心'),
    ('md', 61, u'肿瘤中医科'),
    ('me', 62, u'基层全科'),
    ('na', 63, u'精神科'),
    ('nb', 64, u'心理科'),
    ('nc', 65, u'基层全科'),
    ('oa', 66, u'中医内科'),
    ('ob', 67, u'中医外科'),
    ('oc', 68, u'中医妇科'),
    ('od', 69, u'中医男科'),
    ('oe', 70, u'中医儿科'),
    ('of', 71, u'基层全科'),
    ('qa', 72, u'检验科'),
    ('qb', 73, u'放射科'),
    ('qc', 74, u'内镜科'),
    ('qd', 75, u'病理科'),
    ('qe', 76, u'心电图科'),
    ('qf', 77, u'超声科'),
    ('qg', 78, u'麻醉科'),
    ('qh', 79, u'体检中心'),
    ('qi', 80, u'预防保健科'),
    ('qj', 81, u'基层全科'),
]

FIRST_CLASS_CLINIC_NO = (
    '1',
    '2',
    '3',
    '4',
    '6',
    '7',
    '8',
    '9',
    '11',
    '12',
    '13',
    '14',
    '15',
    '16',
    '17',
    '21',
    '19',
    '22',
)

CLINIC_NO_MAP = {
    # 二级科室到一级科室的对应
    'fa': '2',  # 小儿科
    'fb': '2',  # 新生儿科
    'fc': '2',  # 基层全科

    'aa': '3',  # 呼吸内科
    'ab': '3',  # 心血管内科
    'ac': '3',  # 神经内科
    'ad': '3',  # 消化内科
    'ae': '3',  # 肾内科
    'af': '3',  # 内分泌与代谢科
    'ag': '3',  # 风湿免疫科
    'ah': '3',  # 血液病科
    'ai': '3',  # 感染科
    'aj': '3',  # 基层全科

    'ha': '4',  # 皮肤科
    'hb': '4',  # 性病科
    'hc': '4',  # 基层全科

    'ca': '7',
    'cb': '7',
    'cc': '7',
    'cd': '7',

    'ba': '9',
    'bb': '9',
    'bc': '9',
    'bd': '9',
    'be': '9',
    'bf': '9',
    'bg': '9',
    'bh': '9',
    'bi': '9',
    'bj': '9',
    'bk': '9',

    'ma': '11',
    'mb': '11',
    'mc': '11',
    'md': '11',
    'me': '11',

    'oa': '12',
    'ob': '12',
    'oc': '12',
    'od': '12',
    'oe': '12',
    'of': '12',

    'ja': '14',
    'jb': '14',
    'jc': '14',
    'jd': '14',

    'na': '17',
    'nb': '17',
    'nc': '17',

    'qa': '22',
    'qb': '22',
    'qc': '22',
    'qd': '22',
    'qe': '22',
    'qf': '22',
    'qg': '22',
    'qh': '22',
    'qi': '22',
    'qj': '22',

}

FIELD_MAP_ONLINE = {
    "medicaldb_newoperations": ["id", "name", "abbr", "alias"],
    "medicaldb_newcheckups": ["id", "name", "abbr", "alias"],
    "medicaldb_bodypart": ["id", "name", "abbr"],
    "medicaldb_clinic": ["id", "name", "abbr"],

    "medicaldb_newsymptoms": ["id", "name", "relation_drug", "frequency", "abbr", "alias"],
    "medicaldb_newdiseases": ["id", "name", "relation_drug", "frequency", "abbr", "alias"],

    "medicaldb_newdrugs": ["id", "cn_name", "common_name", "abbr", "alias"]
}

FIELD_MAP_OFFLINE = {
    "medicaldb_newoperations": ["id", "name"],
    "medicaldb_newcheckups": ["id", "name"],
    "medicaldb_bodypart": ["id", "name"],
    "medicaldb_clinic": ["id", "name"],

    "medicaldb_newsymptoms": ["id", "name", "relation_drug", "frequency"],
    "medicaldb_newdiseases": ["id", "name", "relation_drug", "frequency"],

    "medicaldb_newdrugs": ["id", "cn_name", "common_name"]
}

CATE_MAP = {
    "medicaldb_newoperations": u"OPERATION_DESC",
    "medicaldb_newcheckups": u"CHECKUP_DESC",

    "medicaldb_bodypart": u"BODYPART_DESC",
    "medicaldb_clinic": u"CLINIC_DESC",

    "medicaldb_newsymptoms": u"SYMPTOM_DESC",
    "medicaldb_newdiseases": u"DISEASE_DESC",

    "medicaldb_newdrugs": u"DRUG_DESC",
}





