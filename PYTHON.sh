#!/bin/sh

PWD=`pwd`

source ${PWD}/scripts/_common.sh
export PYTHONPATH=$PYTHONPATH:$PWD

${ENV_PATH}/bin/python $@

