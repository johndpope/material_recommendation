# encoding=utf8

import sys
import random
# from medical_ner_offline.segment_ner_manager import MedicalNerHandler
# from medical_ner_offline.update import get_data

texts = [u"肺气肿 舌 种植牙脱落，阳性 手掌皮下红肿 右下腹痛 y-现象和菱形现象 第三 托-亨二氏综合征 新生儿肺弥散功能障碍",
         u"肺弥散功能障碍b超唐筛 左手 右手 眼睛 眉毛 种植牙"]


# def test_medical_ner():
#     print '#' * 20, 'test_medical_ner', '#' * 20
#     flags = {"relation_drug_num": 5, "freq": 1}
#     for i in range(2):
#         print "=" * 10
#         text = random.choice(texts)
#
#         o = MedicalNerHandler.medical_ner_single(text, flags)
#         for token_info in o:
#             print '\t'
#             for x in token_info:
#                 print x, token_info[x]
#
#     tokens_list = MedicalNerHandler.medical_ner(texts, flags)
#     for tokens in tokens_list:
#         print '-' * 20
#         for token_info in tokens:
#             print '\t'
#             for x in token_info:
#                 print x, token_info[x]
#
#
# def test_tokenizer():
#     print '#' * 20, 'test_tokenizer', '#' * 20
#     flags = ['ner', 'pos', 't2s', 'new_word', 'lower']
#
#     for i in range(2):
#         print "=" * 10
#         text = random.choice(texts)
#
#         o = MedicalNerHandler.tokenizer_single(text, flags)
#         for token_info in o:
#             print '\t'
#             for x in token_info:
#                 print x, token_info[x]
#
#     tokens_list = MedicalNerHandler.tokenizer(texts, flags)
#     for tokens in tokens_list:
#         print '*' * 20
#         for token_info in tokens:
#             print '\t'
#             for x in token_info:
#                 print x, token_info[x]


# def test_update():
#     print '#' * 20, 'test_update', '#' * 20
#     get_data(is_test=False)

def test2():
    from openpyxl import load_workbook
    from general_utils.time_utils import datetime_str2timestamp
    filename = '/Users/satoshi/Documents/work file/feed_news_show_list.xlsx'
    wb = load_workbook(filename=filename)
    sheet_names = wb.get_sheet_names()
    print sheet_names
    sheet_name = sheet_names[0]
    sheet = wb.get_sheet_by_name(sheet_name)
    rows = sheet.rows
    columns = sheet.columns
    for row in rows:
        line = [col.value for col in row]
        try:
            id = int(line[0])
        except:
            continue
        t = line[6]
        if u"前" in t:
            t = '2018-02-27'
        elif u'月' in t:
            t = '2017-0' + t.split(u'月')[0] + '-' + t.split(u'月')[1].split(u'日')[0]
        print id,t,datetime_str2timestamp(t)

def test3():
    import openpyxl
    from general_utils.file_utils import read_from_xlsx_file
    file_name = 'bracket_words_lirui.xlsx'
    res = read_from_xlsx_file(file_name)





def generate_partner_name_file():
    from general_utils.file_utils import read_from_xlsx_file
    inputfile = "/Users/satoshi/Downloads/温欣驿站项目医生名单--春雨医生.xlsx"
    data = read_from_xlsx_file(inputfile)[1:]
    doctor_list = []
    partner_name = 'qiangshengxinpuni'
    for x in data:
        doctor_list.append(x[3])
    stri = partner_name + '#' + ','.join(doctor_list)
    file_name_o = "/Users/satoshi/Downloads/partner_doctor_list.txt"
    with open(file_name_o, 'w') as f:
        f.write(stri+'\n')

if __name__ == '__main__':
    generate_partner_name_file()
