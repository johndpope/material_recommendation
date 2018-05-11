# -*- coding:utf-8 -*-



import os
import json
import cPickle as pickle

import openpyxl

from chunyu.utils.general.encoding_utils import ensure_unicode


def pickle_to_str(obj):
    return pickle.dumps(obj)


def pickle_from_str(s):
    return pickle.loads(s)


def pickle_to_file(obj, filename):
    with open(filename, 'w') as f:
        pickle.dump(obj, f)


def pickle_from_file(filename):
    print "pickle_from_file", filename
    with open(filename) as f:
        return pickle.load(f)


def load_simple_lines(filename):
    res = set()
    with open(filename, 'r') as f:
        for l in f:
            l = l.strip('\n').strip()
            res.add(ensure_unicode(l))
    print "load from file %s" % filename
    return res


def load_from_file(filename, limit=None, to_unicode=True):
    res = []
    with open(filename, 'r') as f:
        for i, line in enumerate(f):
            if limit and i >= limit - 1:
                break
            line = line.strip()
            if to_unicode:
                line = ensure_unicode(line)
            if line:
                res.append(line)

    return res


def get_files(dirname, substr=None):
    res = []
    for i in next(os.walk(dirname))[2]:
        if not substr or substr in i:
            res.append(os.path.join(dirname, i))
    return res


def remove_by_dir(dirname):
    files = get_files(dirname)
    for f in files:
        file_name = os.path.join(dirname, f)
        os.remove(file_name)


def read_from_xlsx_file(filename, sheet_index=0):
    '''

    :param filename:
    :param sheet_index: 读取sheet的序号
    :return: list of list
    '''
    wb = openpyxl.load_workbook(filename=filename)
    sheet_names = wb.get_sheet_names()
    sheet_name = sheet_names[sheet_index]
    sheet = wb.get_sheet_by_name(sheet_name)
    rows = sheet.rows
    res = []
    for row in rows:
        res.append([col.value for col in row])
    return res
