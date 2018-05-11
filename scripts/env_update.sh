#!/bin/sh

PWD=`pwd`
source ${PWD}/scripts/_common.sh

if [ ! -f ${ENV_PATH}/bin/activate ]; then
    sh ${PWD}/scripts/env_prepare.sh
fi

source ${ENV_PATH}/bin/activate
SMART_UPDATE=${ENV_PATH}/bin/smart_update.py
echo ${ECHO_EXT} "${Red}更新依赖包: ${Gre}${SMART_UPDATE} --update ${RCol} >>>>>>>"

$SMART_UPDATE --update
