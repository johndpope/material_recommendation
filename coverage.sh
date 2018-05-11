#!/bin/sh

#
# 默认采用线上环境来测试
# 假定: coverage存在的路径如下
#  线上机器:   /home/chunyu/workspace/ENV/bin/coverage
#  开发者机器: ~/workspace/ENV/bin/coverage
#  系统目录:   coverage
#

# 如果失败，则手动创建
# sudo mkdir -p /var/log/redis_test/
# sudo chmod 777 /var/log/redis_test/

if [ ! -f settings.py ]; then
    cp settings-test.py settings.py
fi
if [ ! -d /tmp/medweb_test ]; then
    mkdir -p /tmp/medweb_test
fi
if [ ! -d log ]; then
    mkdir log
fi

# 先关闭，在重启
redis-cli SHUTDOWN NOSAVE
redis-server conf/test_redis.conf

COVERAGE_PATH="/home/chunyu/workspace/ENV/bin/coverage"
if [ ! -f $COVERAGE_PATH ]; then

    # 注意: ~ 不要放在引号内部
    COVERAGE_PATH=~/workspace/ENV/bin/coverage

    if [ ! -f $COVERAGE_PATH ]; then
        COVERAGE_PATH="coverage"
    fi
fi

echo "COVERAGE PATH: ${COVERAGE_PATH}"

# 带测试的模块
COVERAGE_APPS="medicaldb"

# 忽略的文件
OMIT_PATHS="/Library/*,/System/*,*/test*,*/testing/*,*/admin*,*/commands/*"

# 关闭redis
# http://redis.io/commands/shutdown
# redis-cli SHUTDOWN NOSAVE
#删除sqlite3.db
rm -rf ./sqlite3.db
#根据model创建db
echo no | ./PYTHON.sh manage.py syncdb --settings='settings-test'
# 执行coverage测试
./PYTHON.sh manage.py test --noinput --settings=settings-test ${COVERAGE_APPS}
