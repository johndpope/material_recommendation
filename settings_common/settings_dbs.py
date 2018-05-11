# -*- coding:utf-8 -*-
import os

from settings_common.settings_base import PROJECT_ROOT

SOUTH_DATABASE_ADAPTERS = {
    'default': 'south.db.mysql'
}

DATABASES_ONLINE = {
    'sqlite': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(PROJECT_ROOT, 'sqlite3.db'),
        'USER': '',
        'PASSWORD': '',
        'HOST': '',
        'PORT': '',
    },

    # 写的请求量稍微少一些，6%的请求为写请求, 因此不保持长连接
    'default': {
        # mysql -h 10.9.76.21 -u medical -p768768cyTX
        'ENGINE': 'django_mysqlpool.backends.mysqlpool',
        'NAME': 'online_medical',
        # 'USER': 'chunyu',
        'USER': 'medical',
        'PASSWORD': '768768cyTX',
        'HOST': 'medweb_mysql_vip.chunyu.me',   # db05: 10.215.33.5/3306 从库
        'PORT': '23306',
        'OPTIONS': {'charset': 'utf8mb4', "autocommit": True},
    },
}

DATABASES_BIGDATA_OFFLINE = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'test_medical',
        'USER': 'bigdata',
        'PASSWORD': '123456',
        'HOST': '10.215.33.12',
        'PORT': '3306',
        'OPTIONS': {
            'charset': 'utf8mb4', },
    },
}

DATABASES_BIGDATA = {
    # mysql -h 10.215.33.12 -u bigdata -p
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'online_medical',
        'USER': 'bigdata',
        'PASSWORD': '123456',
        'HOST': '10.215.33.12',
        'PORT': '3306',
        'OPTIONS': {
            'charset': 'utf8mb4', },
    },
}

DATABASES_BIGDATA_NEW = {
    # mysql -umedical_entity -pchunyu@2017#3316 -h10.215.33.152 -P13316
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'medical_entity',
        'USER': 'medical_entity',
        'PASSWORD': 'chunyu@2017#3316',
        'HOST': '10.215.33.152',
        'PORT': '13316',
        'OPTIONS': {
            'charset': 'utf8mb4', },
    },
}

DATABASE_NEWS = {
    'sqlite': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(PROJECT_ROOT, 'sqlite3.db'),
        'USER': '',
        'PASSWORD': '',
        'HOST': '',
        'PORT': '',
    },
    'default': {
        # healthnews存的地方
        # mysql -h 10.215.33.151 -u news -P 13310 -p768768cyTX
        'ENGINE': 'django_mysqlpool.backends.mysqlpool',
        'NAME': 'online_news',
        'USER': 'news',
        'PASSWORD': '768768cyTX',

        'HOST': '10.215.33.151',
        'PORT': '13310',
        'OPTIONS': {'charset': 'utf8mb4', "autocommit": True},
    },
}

DATABASES_TEST = {

    # 写的请求量稍微少一些，6%的请求为写请求, 因此不保持长连接
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'test_medical',
        'USER': 'test',
        'PASSWORD': 'testcyTX',
        'HOST': 'mysqlbiz',
        'PORT': '3306',

        'OPTIONS': {
            'charset': 'utf8mb4', },
    },
}

DATABASE_ONLINE_MEDICAL = {
    'default':
        {
            #mysql -h medweb_mysql_vip.chunyu.me -u chunyu -P 23306 -p768768cyTX
            'ENGINE': 'django_mysqlpool.backends.mysqlpool',
            'NAME': 'online_medical',
            'USER': 'material',
            'PASSWORD': 'mordoreheaketrustrul',

            'HOST': 'medweb_mysql_vip.chunyu.me',
            'PORT': '23306',
            'OPTIONS': {'charset': 'utf8mb4', "autocommit": True},

        # 'ENGINE': 'django_mysqlpool.backends.mysqlpool',
        # 'NAME': 'online_medical',
        # 'USER': 'chunyu',
        # 'PASSWORD': '768768cyTX',
        # #mysql -h medweb_mysql.chunyu.me -u chunyu -P 3306 -p768768cyTX
        # 'HOST': 'medweb_mysql.chunyu.me',
        # 'PORT': '3306',
        # 'OPTIONS': {'charset': 'utf8mb4', "autocommit": True},
        }
}


DATABASE_DIAGNOSE = {
    'default': {
        # mysql -h medweb_mysql.chunyu.me -u work -pwork
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'disease_diagnose',
        'USER': 'work',
        'PASSWORD': 'work',
        'HOST': 'medweb_mysql.chunyu.me',
        'PORT': '',
        'OPTIONS': {
            'autocommit': True,
            "init_command": "SET foreign_key_checks = 0;",
            'charset':'utf8mb4',
        },
    },
}

