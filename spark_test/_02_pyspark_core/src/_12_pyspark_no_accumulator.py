# 目的: 演示累加器, 注意: 目前只有 加 的功能.

# 导包.
from pyspark import SparkConf, SparkContext
import os
import time

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('PySpark模板')

    # 1. 创建Spark的核心对象
    conf = SparkConf().setAppName('wordCount').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 注意: 广播变量是只读的, 各个线程只能读取数据, 不能修改数据.
    # 版本1: 演示不使用 广播变量的情况.
    # 定义1个变量.
    a = 10

    # 2. 获取rdd对象.
    rdd_init = sc.parallelize(range(1, 11))


    # 3. 处理数据, 把列表中的数据, 都和a累加到一起.
    def fn1(num):
        global a  # 全局变量
        a += num
        return num


    # 4. 具体的执行流程.
    print(rdd_init.map(fn1).collect())  #   [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # 看看变量a的变量.
    print(a)  # 10,  遇到问题了, 我们本意是要把 列表中的元素值累计给变量a, 但是发现, 没有成功, 如何解决呢?  累加器.
