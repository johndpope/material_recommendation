# -*- coding:utf-8 -*-

CHUNYU_APPS = (
    'recommend',

)

## 请 不要直接修改 INSTALLED_APPS，如果要添加App请在 CHUNYU_APPS 中添加
## 这里的APPS不会参与South的model的升级
INSTALLED_APPS = (
                     'raven.contrib.django.raven_compat',
                     'reversion',
                     'django.contrib.contenttypes',
                 ) + CHUNYU_APPS
