# encoding=utf8


import MySQLdb as ms
from medweb_utils.utilities.log_utils import info_logger


def parse_dbconfig(settings_dbconfig):
    simple_dbconfig = {}
    simple_dbconfig['host'] = settings_dbconfig['default']['HOST']
    simple_dbconfig['db'] = settings_dbconfig['default']['NAME']
    simple_dbconfig['user'] = settings_dbconfig['default']['USER']
    simple_dbconfig['passwd'] = settings_dbconfig['default']['PASSWORD']
    simple_dbconfig['port'] = int(settings_dbconfig['default']['PORT'])
    simple_dbconfig['charset'] = settings_dbconfig['default']['OPTIONS']['charset']
    return simple_dbconfig


class DoMany(object):
    def __init__(self, dbconfig):

        if dbconfig['port']:
            self.conn = ms.connect(host=dbconfig['host'],
                                   user=dbconfig['user'],
                                   passwd=dbconfig['passwd'],
                                   port=dbconfig['port'],
                                   db=dbconfig['db'],
                                   charset=dbconfig['charset'])
        else:
            self.conn = ms.connect(host=dbconfig['host'],
                                   user=dbconfig['user'],
                                   passwd=dbconfig['passwd'],
                                   db=dbconfig['db'],
                                   charset=dbconfig['charset'])

    def __del__(self):
        self.conn.close()

    def do_one(self, sql, commit=False):
        cur = self.conn.cursor()
        # print "sql"
        # print sql
        try:
            cur.execute(sql)
            return cur.fetchall()
        except Exception, e:
            print "Exception:", e
            info_logger.info("sql exception %s", e)
            return None


# def test():
#     import datetime
#     t = datetime.datetime(2018, 3, 19, 17, 02)
#     test_medical_config = {
#     'default': {
#         # mysql -h 10.215.33.12 -u bigdata -p123456
#         'ENGINE': 'django.db.backends.mysql',
#         'NAME': 'test_medical',
#         'USER': 'bigdata',
#         'PASSWORD': '123456',
#         'HOST': '10.215.33.12',
#         'PORT': '3306',
#         'OPTIONS': {
#             'charset': 'utf8mb4',
#         },
#     },
#     }
#     dbconfig = parse_dbconfig(test_medical_config)
#     h = DoMany(dbconfig)
#     f_names = ['cn_name','created_time','last_modified','comment_num']
#     indication = "本品用于预防和治疗季节性过敏性鼻炎(包括枯草热)和常年性过敏性鼻炎。"
#     sql = 'select %s from medicaldb_newdrugs where cn_name="辅舒良";'%','.join(f_names)
#     sql1 = 'insert into medicaldb_newdrugs(common_name,created_time,last_modified,comment_num,indication) values("辅舒良","%s","%s",0,"%s");'%(t,t,indication)
#     print sql1
#     o = h.do_one(sql)
#     print o

def test():
    import json
    online_medical_config = {
        'default': {
            # mysql -h 10.215.33.12 -u bigdata -p123456
            'ENGINE': 'django.db.backends.mysql',
            'NAME': 'online_medical',
            'USER': 'chunyu',
            'PASSWORD': '768768cyTX',
            'HOST': '10.9.16.251',
            'PORT': '3306',
            'OPTIONS': {
                'charset': 'utf8mb4',
            },
        },
    }

    dbconfig = parse_dbconfig(online_medical_config)
    h = DoMany(dbconfig)
    file_name = 'mysql_data_medicaldb_newdrugs'
    f = open(file_name, 'w')
    sql = 'select id,common_name,indication from medicaldb_newdrugs;'
    o = h.do_one(sql)
    for item in o:
        id = item[0]
        common_name = item[1]
        indication = item[2]

        jsonobj = json.dumps(
            {
                'id': id,
                'common_name': common_name,
                'indication':indication,
            }
        )
        f.write(jsonobj + '\n')
    f.close()


if __name__ == '__main__':
    test()
