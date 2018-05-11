# encoding=utf8
from __future__ import absolute_import
import json
from rpc_services import rpc_clients


def tokenizer(texts, flags):
    input_arg = json.dumps({"texts": texts, "flags": flags})
    return json.loads(rpc_clients.get_medical_service_client().tokenizer(input_arg))


def tokenizer_default(texts):
    flags = ["ner", "neg_ner"]
    return tokenizer(texts, flags)


def medical_ner(texts, flags):
    input_arg = json.dumps({"texts": texts, "flags": flags})
    return json.loads(rpc_clients.get_medical_service_client().medical_ner(input_arg))


# def test():
#     import csv
#     from general_utils.text_utils import convert2gbk
#     fi = open("/Users/satoshi/Documents/work file/one_week_query.csv","r")
#     fo = open("/Users/satoshi/Documents/work file/one_week_query_3.csv","w")
#     s = False
#     csvwriter = csv.writer(fo)
#     for l in fi:
#
#         l = l.decode("gbk","ignore").strip('\n')
#         ll = l.split(',')
#         if len(ll) == 5:
#             t,text,id = ll[0],ll[1],ll[2]
#         else:
#             t = l.split(',')[0]
#             try:
#                 text = l.split('"')[1]
#                 print l
#             except:
#                 continue
#             id = l.split(',')[-3]
#         if id == "5c8325d00676977f":
#             s = True
#         if not s:
#             continue
#         tokens = tokenizer_default([text])["tokens"][0]
#
#         words = []
#         cates = []
#         for item in tokens:
#             if u'cate' in item:
#                 if u"neg_ne" in item:
#                     continue
#                 word = item['token']
#                 if word in words:
#                     continue
#                 words.append(word)
#                 cates.append(item['cate'])
#         rows = [t,text,id,u"|||".join(words),u"|||".join(cates)]
#         rows = convert2gbk(rows)
#         csvwriter.writerow(rows)
#     fo.close()
#     fi.close()
