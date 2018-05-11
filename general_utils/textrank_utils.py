# encoding=utf8
import sys

from textrank4zh.util import sort_words


def cal_tags_textrank(node_tags, edge_tags):
    # node_tags 二维是，edge_tags也是
    # return a dict tag:weight
    tag_rank = {}
    o = sort_words(node_tags, edge_tags,3)
    for item in o:
        tag_rank[item['word']] = item['weight']
    return tag_rank
