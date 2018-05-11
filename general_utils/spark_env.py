# -*- encoding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark import StorageLevel
import random

conf = SparkConf()
conf.setExecutorEnv('PYSPARK_PYTHON', '/home/classify/workspace/ENV_material_recommendation/bin/python')
conf.setExecutorEnv('PYTHONPATH',
                    '/home/classify/workspace/ENV_material_recommendation/lib/python2.7/site-packages:/home/classify/workspace/material_recommendation')


# conf.setExecutorEnv('PYSPARK_PYTHON', '/home/classify/workspace/ENV_qa-helper/bin/python')
# conf.setExecutorEnv('PYTHONPATH',
#                     '/home/classify/workspace/ENV_qa-helper/lib/python2.7/site-packages:/home/classify/workspace/qa-helper')

conf.set("spark.cores.max", "40")
conf.set("spark.scheduler.mode", "FAIR")

sc = SparkContext(conf=conf)

sc.setLogLevel('ERROR')


def custom_zip(rdd1, rdd2, npart=None):
    """
    see http://stackoverflow.com/questions/32084368/can-only-zip-with-rdd-which-has-the-same-number-of-partitions-error
    """

    def prepare(rdd, npart):
        return (rdd.zipWithIndex()
                .map(lambda x: (x[1], x[0])).partitionBy(npart, lambda x: x % npart)
                .values())

    if not npart:
        npart = max(rdd1.getNumPartitions(), rdd2.getNumPartitions())
    return prepare(rdd1, npart).zip(prepare(rdd2, npart))


def shuffle_rdd(rdd1):
    return rdd1.map(lambda x: (x, random.random())).sortBy(lambda x: x[1]).map(lambda x: x[0])


def test():
    from operator import add
    inputfile = "/user/classify/ctr_train_data/test_text"
    inputrdd = sc.textFile(inputfile)
    mid_rdd1 = inputrdd.map(lambda x: (x, 1))
    mid_rdd2 = mid_rdd1.reduceByKey(add)
    mid_rdd3 = mid_rdd2.map(lambda x: (x[1], x[0]))
    mid_rdd4 = mid_rdd3.sortByKey(ascending=False)
    mid_rdd5 = mid_rdd4.map(lambda x: (x[1], x[0])).keys()
    top2 = mid_rdd5.take(2)
    print '|'.join(top2)


if __name__ == '__main__':
    test()