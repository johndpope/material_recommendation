# -*- coding:utf-8 -*-
from settings_common.settings_base import *
from settings_common.settings_install_apps import *

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(PROJECT_ROOT, 'sqlite3.db'),
        'USER': '',
        'PASSWORD': '',
        'HOST': '',
        'PORT': '',
    }
}

IS_FOR_TESTCASE = True
IS_ONLINE_WEB_SERVER = False
