# encoding=utf8

import numpy as np
import random
import time
import math


def test1():
    # 看ndarray和list谁归一化快一些
    # 结论：list比ndarray快
    vec = [random.random() * random.randint(1, 5) for _ in range(256)]
    nvec = np.array(vec)

    t1 = time.time()
    s = math.sqrt(sum([x * x for x in vec]))
    n_vec = [x / s for x in vec]
    t2 = time.time()

    n_nvec = nvec / np.linalg.norm(nvec)
    #n_nvec.tolist()
    t3 = time.time()

    print t2-t1,np.linalg.norm(np.array(n_vec))
    print t3-t2,np.linalg.norm(n_nvec)

if __name__ == '__main__':
    test1()
