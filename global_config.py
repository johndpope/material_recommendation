# -*- coding:utf-8 -*-
import os
import settings
import socket


def get_root_path():
    return os.path.dirname(os.path.realpath(__file__))


def get_log_path():
    data_dir = os.path.join(get_root_path(), 'log')
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    return data_dir


def get_data_path():
    data_dir = os.path.join(get_root_path(), 'data_dir/')
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    return data_dir


def get_hostname():
    return socket.gethostname()
