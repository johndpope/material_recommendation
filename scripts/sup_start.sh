#!/bin/sh
PWD=`pwd`
source ${PWD}/scripts/_common.sh

SUPERVISOR_FILE="${PWD}/supervisord.conf"
${ENV_PATH}/bin/supervisord -c ${SUPERVISOR_FILE}