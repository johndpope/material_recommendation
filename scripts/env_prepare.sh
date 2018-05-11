#!/bin/sh

PWD=`pwd`
source ${PWD}/scripts/_common.sh

echo ${ECHO_EXT} "${Red}初始化开始: ${Gre}${ENV_PATH}${RCol} >>>>>>>"

#
# 线上服务器的Python都为2.7, 且位置在: /usr/local/python2.7
#
if [ -f /usr/local/python27/bin/virtualenv ]; then
    VIRTUAL_ENV=/usr/local/python27/bin/virtualenv
else
    VIRTUAL_ENV=`which virtualenv`
fi

${VIRTUAL_ENV} ${ENV_PATH}

source ${ENV_PATH}/bin/activate

SITE_CUSTOMIZE="${ENV_PATH}/lib/python2.7/site-packages/sitecustomize.py"
if [ ! -f ${SITE_CUSTOMIZE}  ]; then
    cat >> ${SITE_CUSTOMIZE} << "EOF"
# -*- coding:utf-8 -*-
#
# 设置系统的默认编码, 这样utf-8和unicode之间就可以自由转换了；否则系统默认的编码为ascii
#
import sys
sys.setdefaultencoding('utf-8')
EOF
fi

pip install colorama
pip install -i http://da0292f9143e49aca905a643b0c54466:e9f9b93756eb47bca3d8b942c4cecbfc@pypi.chunyu.mobi/simple cy-pypi==1.1.6
pip install pip==1.5.6

deactivate

echo ${ECHO_EXT} "${Red}初始化完毕: ${Gre}${ENV_PATH}${RCol} <<<<<<"
