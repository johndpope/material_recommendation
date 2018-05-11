# -*- encoding:utf-8 -*-
"""
基础配置文件
1、数据库配置
2、线上环境类型管理
"""


class ServerEnvironmentType(object):
    """
    服务环境：线上，biz测试等
    """
    ONLINE = "online"
    TEST = "test"
    BIZ_TEST = "biztest"
    QC_TEST = "qctest"
    LOCAL_TEST = "localtest"
    TESTCASE = "testcase"
    MD4 = "md4"
    TYPE_SET = [ONLINE, TEST, BIZ_TEST, QC_TEST, LOCAL_TEST, TESTCASE,MD4]

    @classmethod
    def is_online_server(cls, server_type):
        """
        是否是线上环境
        """
        return server_type == cls.ONLINE

    @classmethod
    def is_testcase(cls, server_type):
        """
        是否是线上环境
        """
        return server_type == cls.TESTCASE

    @classmethod
    def is_valid_server(cls, server_type):
        """
        是不是合理的服务，保证调用方的正确
        """
        return server_type in cls.TYPE_SET


def get_medweb_database_info(server_type):
    """
    获取Medweb数据库的相关信息
    """
    keys = ["host", "port", "user", "password", "database"]
    if server_type == ServerEnvironmentType.ONLINE:
        conf_list = ["10.215.33.5", "3306", "chunyu", "768768cyTX", "online_medical"]
    elif server_type == ServerEnvironmentType.BIZ_TEST:
        conf_list = ["mysqlbiz", "3306", "test", "testcyTX", "test_medical"]
    else:
        conf_list = ["mysqltest", "3306", "test", "testcyTX", "test_medical"]
    return dict(zip(keys, conf_list))


