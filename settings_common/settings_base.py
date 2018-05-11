# -*- coding:utf-8 -*-

import logging

import os

PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

TIME_ZONE = 'Asia/Shanghai'
LANGUAGE_CODE = 'zh-cn'
SITE_ID = 1
USE_I18N = True
USE_L10N = True
USE_ETAGS = True

ROOT_URLCONF = 'urls'
STATIC_URL = '/static/'

STATIC_ROOT = PROJECT_ROOT + '/static/'
MEDIA_ROOT = PROJECT_ROOT + '/media/'

TEMPLATE_DIRS = (
    PROJECT_ROOT + '/templates',
)

SECRET_KEY = 'dazk&z4h2u8r(+x+!gq)u4t$#(4%x)ey((84^585k=g+zsh8$1'

# MIDDLEWARE_CLASSES = (
#     'django.middleware.common.CommonMiddleware',
#     'django.contrib.messages.middleware.MessageMiddleware',
# )

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
    'django.template.loaders.eggs.Loader'
)

LOGS_BASE_DIR = os.path.join(PROJECT_ROOT, "log")
if not os.path.exists(LOGS_BASE_DIR):
    os.mkdir(LOGS_BASE_DIR)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,

    'formatters': {
        'verbose': {
            'format': '%(asctime)s %(levelname)s %(module)s.%(funcName)s Line:%(lineno)d  %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },

    'handlers': {
        'null': {
            'level': 'DEBUG',
            'class': 'django.utils.log.NullHandler',
        },
        ## 默认的服务器Log(保存到log/filelog.log中, 通过linux的logrotate来处理日志的分割
        'default': {
            'level': 'INFO',
            'class': 'medweb_utils.log.log_handler.CustomWatchedFileHandler',
            'filename': os.path.join(LOGS_BASE_DIR, 'filelog.log'),
            'formatter': 'verbose',
        },

        ## 默认的服务器ERROR log
        'default_err': {
            'level': 'ERROR',
            'class': 'medweb_utils.log.log_handler.CustomWatchedFileHandler',
            'filename': os.path.join(LOGS_BASE_DIR, 'error_logger.log'),
            'formatter': 'verbose',
        },
        'info_logger': {
            'level': 'INFO',
            'class': 'medweb_utils.log.log_handler.CustomWatchedFileHandler',
            'filename': os.path.join(LOGS_BASE_DIR, 'info_logger.log'),
            'formatter': 'verbose',
        },
        'exception_logger': {
            'level': 'INFO',
            'class': 'medweb_utils.log.log_handler.CustomWatchedFileHandler',
            'filename': os.path.join(LOGS_BASE_DIR, 'exception_logger.log'),
            'formatter': 'verbose',
        },
        'elapsed_logger': {
            'level': 'INFO',
            'class': 'medweb_utils.log.log_handler.CustomWatchedFileHandler',
            'filename': os.path.join(LOGS_BASE_DIR, 'elapsed_logger.log'),
            'formatter': 'verbose',
        },
        'ticker_logger': {
            'level': 'INFO',
            'class': 'medweb_utils.log.log_handler.CustomWatchedFileHandler',
            'filename': os.path.join(LOGS_BASE_DIR, 'ticker_logger.log'),
            'formatter': 'verbose',
        },
    },

    'loggers': {
        'django': {
            'handlers': ['default'],
            'propagate': True,
            'level': 'INFO',
        },
        'django.request': {
            'handlers': ['default_err'],
            'level': 'ERROR',
            'propagate': False,
        },
        'info_logger': {
            'handlers': ['info_logger'],
            'level': 'INFO',
            'propagate': False,
        },
        'exception_logger': {
            'handlers': ['exception_logger'],
            'level': 'INFO',
            'propagate': False,
        },
        'default_logger': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True,
        },
        'elapsed_logger': {
            'handlers': ['elapsed_logger'],
            'level': 'INFO',
            'propagate': False,
        },
        'ticker_logger': {
            'handlers': ['ticker_logger'],
            'level': 'INFO',
            'propagate': False,
        },
    }
}

# 只要执行就OK
logging.root = logging.getLogger('default_logger')

# List of finder classes that know how to find static files in various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
)

TEMPLATE_CONTEXT_PROCESSORS = (
    "django.core.context_processors.debug",
    "django.core.context_processors.i18n",
    "django.core.context_processors.static",
    "django.contrib.auth.context_processors.auth",
    "django.core.context_processors.request",
    "django.core.context_processors.media",
    # "website.website_context",
)

RAVEN_CONFIG = {
    'dsn': 'https://9557037a365c4c25be0d413d00bf82a3:f315133ab7c743c6bdf7871bbe05084a@sentry.chunyu.me/39',
    'enabled': True,
}

HBASE_HOST = 'hd3'

IS_ONLINE_WEB_SERVER = False
IS_FOR_TESTCASE = False

RPC_LOCAL_PROXY = "/usr/local/rpc_proxy/online_proxy.sock"

ZK_HOST = "zk_rpc_2:2181"

