# -*- coding:utf-8 -*-
from medweb_utils.django_ext.south_migration import SouthMigration
from settings_common.settings_base import *
from settings_common.settings_dbs import DATABASES_ONLINE as DATABASES

from settings_common.settings_install_apps import *

MIGRATIONS_PROFILE = "online"
SOUTH_MIGRATION_MODULES = SouthMigration(profile=MIGRATIONS_PROFILE, need_utf8mb4=False,
                                         project_root=PROJECT_ROOT, installed_apps=CHUNYU_APPS)

INSTALLED_APPS += ('south',)

DATABASES['default']['ENGINE'] = 'django.db.backends.mysql'


IS_FOR_TESTCASE = False
IS_ONLINE_WEB_SERVER = False

ZK_HOST = "zk_rpc_1:2181,zk_rpc_2:2181,zk_rpc_3:2181"

HBASE_HOST = 'hbase_server'
HBASE_PORT = 19090


RAVEN_CONFIG = {
    'dsn': 'https://79a87d36fd884f2d82fb6bcc8a9c58ea:cf1107a269a541b3ba32b7b63f96d236@sentry.chunyu.me/33',
    'enabled': True,
}


