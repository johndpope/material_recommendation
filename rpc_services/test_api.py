#encoding=utf8

import os
import time
from cy_seg.jieba_seg import tokenizer
print '-----'
text = '假性肺炎糖尿病b超唐筛'
t1 = time.time()
o = tokenizer(text)
t2 = time.time()
print t2 - t1
for x in o:
    print x,type(x)


# from recommend.manager.recommend_resource import Recommend
#
# o = Recommend(uid=63862248,lookback=5*60.8,end=1510023581500)
#
# print o


