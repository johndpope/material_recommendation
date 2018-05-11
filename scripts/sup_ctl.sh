#!/bin/sh
# 只在服务器上有效

PWD=`pwd`
source ${PWD}/scripts/_common.sh

SUPERVISOR_FILE="${PWD}/supervisord.conf"
${ENV_PATH}/bin/supervisorctl -c ${SUPERVISOR_FILE}