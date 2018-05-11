# encoding=utf8

import math


def ln_rescale(float_num, k=1.0):
    '''
    将0-1之间的float拉伸，效果是使小的数变大的多一些
    保持y[0] = 0 y[1] = 1
    :param float_num:
    :param k:
    :return:
    '''
    min_num = 0.01
    min_value = math.log(k * min_num + 1.0) / math.log(k + 1.0)

    if float_num < 0.01:
        return min_value
    k = float(k)
    return math.log(k * float_num + 1.0) / math.log(k + 1.0)


def test_ln_rescale():
    print ln_rescale(0.9, 50)
    print ln_rescale(0.7, 50)


def yield_batch_data(data_lines, batch_size=1000):
    total_len = len(data_lines)
    batch_num = total_len // batch_size + 1 if total_len % batch_size != 0 else total_len // batch_size
    print 'yield_batch_data batch_num=', batch_num
    for i in range(batch_num):
        start_index = batch_size * i
        end_index = start_index + batch_size
        yield data_lines[start_index: end_index]


def test_yield_batch_data():
    data_lines = range(9999999)
    for batch_data in yield_batch_data(data_lines, 1000):
        print len(batch_data)


if __name__ == '__main__':
    test_yield_batch_data()
