# encoding=utf8


import numpy as np
from math import sqrt, exp
from random import shuffle
from collections import defaultdict


def get_center(vecs):
    # vecs is ndarray
    if len(vecs) == 0:
        return None
    return np.mean(vecs, axis=0)


def get_center_weighted(vecs, weights):
    # input : vec1:weight1
    return np.average(vecs, axis=0, weights=weights)


def euclidean_dist(ndarray1, ndarray2):
    return np.linalg.norm(ndarray1 - ndarray2)


def cos_sim(ndarray1, ndarray2):
    return np.dot(ndarray1, ndarray2) / np.linalg.norm(ndarray1) / np.linalg.norm(ndarray2)


def cos_sim_unit(ndarray1, ndarray2):
    return np.dot(ndarray1, ndarray2)


def get_gassian_p(vecs):
    # vecs.shape = (
    # 再说吧。。
    pass


pi = 3.1415926
c = sqrt(2.0 * pi)
epsilon = 0.2  ##########需要调节的阈值


def gaussian_1(miu, sigma, x):
    # input都是实数,float
    return exp(-(x - miu) * (x - miu) / 2.0 / sigma / sigma) / (c * sigma)


def gaussian_n(miu_vec, sigma_vec, x_vec):
    # miu_vec,sigma_vec,x_vec 都是256维向量
    p = 1.0
    for i in range(len(miu_vec)):
        p *= gaussian_1(miu_vec[i], sigma_vec[i], x_vec[i])
    return p


def simple_relative_distance():
    pass


def remove_anomaly():
    pass


def get_dfar_dnear(vecs1, vecs2):
    # input : 两组vecs
    # output : 它们中最大、最小的距离
    # 尝试 欧氏距离 和 余弦距离
    if len(vecs1) == 0 or len(vecs2) == 0:
        return None, None
    d_near = 9999999.999
    d_far = -9999999.999
    for vec1 in vecs1:
        for vec2 in vecs2:

            d = euclidean_dist(vec1, vec2)
            if d < d_near:
                d_near = d
            if d > d_far:
                d_far = d
    return d_far, d_near


def get_dfar_dnear2(vecs1, vecs2):
    # 返回d_far, d_near，以及vecs1每个向量与vecs2中余弦相似度最大的相似度，和反过来的
    if len(vecs1) == 0 or len(vecs2) == 0:
        return None, None
    d_near = 9999999.999
    d_far = -9999999.999
    m = len(vecs1)
    n = len(vecs2)
    most_similar_1 = [-1.0] * m
    most_similar_2 = [-1.0] * n
    for i in range(m):
        vec1 = vecs1[i]
        for j in range(n):
            vec2 = vecs2[j]
            cos = cos_sim_unit(vec1, vec2)
            d = euclidean_dist(vec1, vec2)
            if cos > most_similar_1[i]:
                most_similar_1[i] = cos
            if cos > most_similar_2[j]:
                most_similar_2[j] = cos
            if d < d_near:
                d_near = d
            if d > d_far:
                d_far = d
    return d_far, d_near, most_similar_1, most_similar_2


def similarity1(d_far, d_near, center1, center2, num_tags=1.0):
    d_center = euclidean_dist(center1, center2)
    if abs(d_far - 0.0) < 0.0001:
        return 1.0
    # print 'pp', d_far, d_near,d_center
    num_tags = 1.0
    d_center *= num_tags
    score0 = (d_center + d_near) / (d_far)  # - d_center)
    # 把score变到0-1区间
    return -score0 / 2.0 + 1.0


def similarity1_2(d_far, d_near, center1, center2, num_tags=1.0):
    d_center = euclidean_dist(center1, center2)
    return d_center / (d_near + d_far + 0.01)


def additional_cos_similarity(most_similar, weights):
    bottom = sum(weights)
    up = sum([most_similar[i] * weights[i] for i in range(len(most_similar))])
    return up / bottom


def vecs_similarity_v1(u_vecs, u_weights, n_vecs, n_weights):
    # 一期上线用的相似度，缺点是增加一个离群点会导致相似度上升
    if len(u_vecs) == 0 or len(n_vecs) == 0:
        return -100.0
    u_center = get_center_weighted(u_vecs, u_weights)
    n_center = get_center_weighted(n_vecs, n_weights)
    d_far, d_near, most_similar_1, most_similar_2 = get_dfar_dnear2(u_vecs, n_vecs)

    return 1.0 * similarity1(d_far, d_near, u_center, n_center, len(n_vecs) / 2.0 + len(u_vecs) / 2.0) \
           + 1.0 * additional_cos_similarity(most_similar_1, u_weights) \
           + 1.0 * additional_cos_similarity(most_similar_2, n_weights)


def vecs_similarity(u_vecs, u_center, n_vecs, n_center):
    # 新闻的center需要重新计算
    if len(u_vecs) == 0 or len(n_vecs) == 0:
        return 0.0
    d_far, d_near = get_dfar_dnear(u_vecs, n_vecs)
    if d_far is None:
        return 0.0
    if d_far == 0:
        return 1.0
    return similarity1(d_far, d_near, u_center, n_center)


def get_cross_nearest_distance(vecs_a, vecs_b):
    m = len(vecs_a)
    n = len(vecs_b)
    # 单位向量最大距离是2.0
    h_a_b = [2.0] * m  # a的每个点在b集中的最近距离
    h_b_a = [2.0] * n  # b的每个点在a集中的最近距离
    for i in range(m):
        for j in range(n):
            d = euclidean_dist(vecs_a[i], vecs_b[j])
            if d < h_a_b[i]:
                h_a_b[i] = d
            if d < h_b_a[j]:
                h_b_a[j] = d
    return h_a_b, h_b_a


def distance2similarity(d, mode=0):
    if mode == 0:
        return 1.0 / (1.0 + float(d))
    if mode == 1:
        return exp(-float(d))
    if mode == 2:  # 仅对于单位向量,这个结果方差比较大，理想√
        return 1.0 - float(d) * float(d) / 2.0


def vecs_similarity_Hausdorff(u_vecs, n_vecs):
    # 欧几里得距离吧
    # 是否需要对 tag过少做惩罚有待商榷
    h_a_b, h_b_a = get_cross_nearest_distance(u_vecs, n_vecs)
    H = max([max(h_a_b), max(h_b_a)])
    one_tag_decay = 1.0 if len(n_vecs) > 1 else 0.75  # 对一个词的物料降权了
    return distance2similarity(H, mode=2) * one_tag_decay


def get_weights_list(keys, weights_dict, vecs_dict):
    vecs_list = []
    weights_list = []
    for key in keys:
        if key in weights_dict and key in vecs_dict:
            vec = vecs_dict[key]
            if vec is None:
                continue
            weight = weights_dict[key]
            vecs_list.append(vec)
            weights_list.append(weight)
    return vecs_list, weights_list


def vecs_similarity_weighted_Hausdorff(u_vecs, u_weights, n_vecs, n_weights,
                                       n_tags=None, n_weights_is_dict=False):
    # 实际上就是H_a_b把最距离换成了加权平均距离
    # if n_weights_is_dict , n_vecs is a dict too, and both key is tag word

    if n_weights_is_dict and n_tags:
        n_vecs, n_weights = get_weights_list(n_tags, n_weights, n_vecs)

    if len(n_vecs) == 0:
        return -1.0

    h_a_b, h_b_a = get_cross_nearest_distance(u_vecs, n_vecs)
    bottom_a = sum(u_weights[:len(u_vecs)])
    bottom_b = sum(n_weights[:len(n_vecs)])

    up_a = sum([u_weights[i] * h_a_b[i] for i in range(len(u_vecs))])
    up_b = sum([n_weights[i] * h_b_a[i] for i in range(len(n_vecs))])

    H = max([up_a / bottom_a, up_b / bottom_b])
    one_tag_decay = 1.0 if len(n_vecs) > 1 else 0.75  # 对一个词的物料降权了
    return distance2similarity(H, mode=2) * one_tag_decay


def vecs_similarity2(u_vecs, u_weights, n_vecs, n_weights):
    # 实际使用的相似度计算入口，更改或优化在这个文件中进行
    if len(u_vecs) == 0 or len(n_vecs) == 0:
        return -1.0
    return vecs_similarity_weighted_Hausdorff(u_vecs, u_weights, n_vecs, n_weights)


def vecs_similarity3(u_vecs, u_weights, n_vecs_dict, n_weights_dict, n_tags):
    if len(u_vecs) == 0 or len(n_vecs_dict) == 0:
        return -1.0

    return vecs_similarity_weighted_Hausdorff(u_vecs=u_vecs,
                                              u_weights=u_weights,
                                              n_vecs=n_vecs_dict,
                                              n_weights=n_weights_dict,
                                              n_tags=n_tags,
                                              n_weights_is_dict=True,
                                              )


def outlier_rejection(vec_dict, TH=0.1):
    # 异常点剔除
    nearest_dict = defaultdict(set)
    keys = vec_dict.keys()
    for i, this_key in enumerate(keys):
        for that_key in keys[i + 1:]:
            print '-' * 20
            print 'this key', this_key, 'that key', that_key
            similarity = cos_sim_unit(vec_dict[this_key], vec_dict[that_key])
            print similarity
            nearest_dict[this_key].add(similarity)
            nearest_dict[that_key].add(similarity)
    keep_keys = []
    for key in nearest_dict:
        max_similarity = max(nearest_dict[key])
        print 'max_similarity', key, max_similarity
        if max_similarity >= TH:
            keep_keys.append(key)
    print 'keep_keys', '-'.join(keep_keys)
    return keep_keys


def few_points_clustering(vec_dict, TH=0.1):
    '''

    :param vec_dict: uword:ndarray
    :param TH:
    :return:
    '''
    clusters = []
    keys = vec_dict.keys()
    shuffle(keys)
    for key in keys:
        # 遍历每个cluster，若余弦相似度大于TH，加入那个cluster，若小于TH，新建cluster
        break_for_flag = False
        print '-' * 20
        print 'key', key
        print 'clusters', clusters
        for i, point_set in enumerate(clusters):

            for point in point_set:
                if cos_sim_unit(vec_dict[point], vec_dict[key]) >= TH:
                    clusters[i].add(key)
                    break_for_flag = True
                    break

            if break_for_flag:
                break
        if not break_for_flag:
            clusters.append(set([key]))

    # print res
    for x in clusters:
        print '*' * 10
        print '-'.join(x)

    return clusters


def get_neighbour_matrix(vec_dict, TH=0.1):
    neighbour_dict = defaultdict(set)
    keys = vec_dict.keys()
    for i, this_key in enumerate(keys):
        neighbour_dict[this_key].add(this_key)
        for that_key in keys[i + 1:]:
            similarity = cos_sim_unit(vec_dict[this_key], vec_dict[that_key])
            if similarity >= TH:
                neighbour_dict[this_key].add(that_key)
                neighbour_dict[that_key].add(this_key)
    return neighbour_dict


def is_apart(clusters, total_num):
    return sum([len(s) for s in clusters]) == total_num


def fuse_clusters(old_clusters):
    new_clusters = []
    for i, cluster in enumerate(old_clusters):
        find_home = False
        for new_cluster in new_clusters:
            if cluster & new_cluster != set():
                new_cluster.update(cluster)
                find_home = True
                break
        if not find_home:
            new_clusters.append(cluster)
    return new_clusters


def smart_threshhold_for_cluster(n):
    if n <= 15:
        return 0.15
    if n <= 25:
        return 0.25
    return 0.3


def few_points_clustering2(vect_dict):
    TH = smart_threshhold_for_cluster(len(vect_dict))
    neighbour_dict = get_neighbour_matrix(vect_dict, TH)
    for x in neighbour_dict:
        print 'neighbour_dict', x, 'nbs', '-'.join(neighbour_dict[x])
    clusters = []
    for key in neighbour_dict:
        neighbour = neighbour_dict[key]
        # 扫描每个cluster
        find_cluster_flag = False
        for cluster in clusters:
            if key in cluster:
                cluster.update(neighbour)
                find_cluster_flag = True
                print 'key in cluster', key
                break
            if neighbour & cluster != set():
                cluster.update(neighbour)
                find_cluster_flag = True
                print 'key neighbour in cluster', key
                break
        if not find_cluster_flag:
            print 'new cluster', key
            clusters.append(neighbour)

    # print res
    for x in clusters:
        print '*' * 10
        print '-'.join(x)

    # join clusters
    # new_clusters = []
    new_clusters = clusters
    total_num = len(vect_dict)
    while not is_apart(new_clusters, total_num):
        print '==' * 10
        for x in new_clusters:
            print '*' * 10
            print '-'.join(x)
        new_clusters = fuse_clusters(new_clusters)

    # print new_clusters
    for x in new_clusters:
        print '*' * 10, 'final'
        print '-'.join(x)

    return new_clusters
