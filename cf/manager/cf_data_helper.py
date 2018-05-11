# encoding=utf8

import sys
import json
import math
import time
from collections import defaultdict
from cf.cf_consts import MATERIAL_CLICK_USRES_FILE, MATERIAL_SORTED_NEIGHBOUR
from cf.manager.cf_hbase_utils import view_actions
from cf.manager.cf_solr_utils import get_view_profile_rowkeys


def save_user_view_info():
    # 统计每个物料被点击的用户uid
    # 获取数据
    end = time.time()
    begin = end - 60 * 86400
    news_row_key_list = get_view_profile_rowkeys(begin, end, mode='news')
    topic_row_key_list = get_view_profile_rowkeys(begin, end, mode='topic')
    print 'news_row_key_list num', len(news_row_key_list)
    print 'topic_row_key_list num', len(topic_row_key_list)
    news_actions0, topic_actions0 = view_actions(news_row_key_list)
    news_actions1, topic_actions1 = view_actions(topic_row_key_list)
    news_actions = news_actions0 + news_actions1
    topic_actions = topic_actions0 + topic_actions1
    print 'news_actions num', len(news_actions)
    print 'topic_actions num', len(topic_actions)

    # 整理数据
    news_views_dict = defaultdict(set)
    topic_views_dict = defaultdict(set)

    for uid, news_id in news_actions:
        news_id = int(news_id)
        news_views_dict[news_id].add(uid)
    for uid, topic_id in topic_actions:
        topic_id = int(topic_id)
        topic_views_dict[topic_id].add(uid)

    # 保存数据
    with open(MATERIAL_CLICK_USRES_FILE, 'w') as fo:
        for news_id in news_views_dict:
            uid_list = list(news_views_dict[news_id])
            fo.write(json.dumps({'id': news_id, 'type': 'news', 'uids': uid_list}) + '\n')
        for topic_id in topic_views_dict:
            uid_list = list(topic_views_dict[topic_id])
            fo.write(json.dumps({'id': topic_id, 'type': 'topic', 'uids': uid_list}) + '\n')


def cos_similarity_kernel(uid_list1, uid_list2):
    # 假余弦相似度
    cross_uids = set(uid_list1) & set(uid_list2)
    up = len(cross_uids)
    if up == 0:
        return 0
    bottom = math.sqrt(float(len(uid_list1))) * math.sqrt(float(len(uid_list2)))
    return up / bottom


def cal_material_similarity():
    # 计算物料两两之间的相似度
    # 读取数据
    with open(MATERIAL_CLICK_USRES_FILE, 'r') as fi:
        ls = fi.readlines()
    # 转换数据格式
    mid_uids_dict = {}
    for l in ls:
        info = json.loads(l.strip('\n'))
        id = info['id']
        type = info['type']
        uids = info['uids']
        unique_id = str(type) + '_' + str(id)
        mid_uids_dict[unique_id] = uids

    # 两两计算
    res_dict = {}
    for this_id in mid_uids_dict:

        res_dict[this_id] = {}
        for that_id in mid_uids_dict:
            if this_id == that_id:
                continue

        if that_id in res_dict and this_id in res_dict[that_id]:
            res_dict[this_id][that_id] = res_dict[that_id][this_id]
            continue

        that_uids = mid_uids_dict[that_id]
        this_uids = mid_uids_dict[this_id]

        similarity = cos_similarity_kernel(this_uids, that_uids)
        if similarity == 0:
            # 不记录没有重叠uid的物料相似度
            continue
        res_dict[this_id][that_id] = similarity

    # 排序并储存
    with open(MATERIAL_SORTED_NEIGHBOUR, 'w') as fo:
        for unique_id in res_dict:
            sorted_neighbour = sorted(res_dict[unique_id].iteritems(), key=lambda x: x[1], reverse=True)
            fo.write(json.dumps({
                'id': unique_id,
                'neighbour': sorted_neighbour,
            }) + '\n')



###########################some scripts
def cal_material_knn():
    '''
    由物料的被点击数据，生成每个物料最邻近的k个别的物料
    使用"余弦距离"，在我笔记本上写着的那个
    物料只有几万个，速度应该比较快
    :return:
    '''
    fin = sys.argv[2]
    fon = fin + '.knn_output'

    # 读取数据，存入字典
    with open(fin, 'r') as f:
        ls = f.readlines()

    material_num = len(ls)
    material_uid_dict = {}

    for l in ls:
        l = l.strip('\n')
        d = json.loads(l)
        id = int(d['id'])
        total_relation_user_num = d['len']
        uids = d['uids']
        material_uid_dict[id] = [uids, total_relation_user_num]

    # 算
    res_dict = {}
    for this_id in material_uid_dict:
        res_dict[this_id] = {}
        for that_id in material_uid_dict:
            if this_id == that_id:
                continue

            if that_id in res_dict and this_id in res_dict[that_id]:
                res_dict[this_id][that_id] = res_dict[that_id][this_id]
                continue

            ni = material_uid_dict[this_id][1]
            nj = material_uid_dict[that_id][1]
            if ni * nj == 0:
                continue
            w_up = len(set(material_uid_dict[this_id][0]) & set(material_uid_dict[that_id][0]))
            if w_up == 0:
                continue
            w_bottom = math.sqrt(float(ni) * float(nj))
            w = w_up / w_bottom
            res_dict[this_id][that_id] = w
            # res_dict[that_id][this_id] = w

    # sort
    output_lines = []
    for id in res_dict:
        this_dict = res_dict[id]
        sorted_data = [x for x in sorted(this_dict.iteritems(), key=lambda x: x[1], reverse=True) if x[1] > 0]
        output_lines.append(json.dumps({'id': id, 'top': sorted_data}) + '\n')

    # save results
    with open(fon, 'w') as f:
        f.writelines(output_lines)


if __name__ == '__main__':
    # save_user_view_info()
    cal_material_similarity()
