# 演示如何创建RDD对象.
# 方式1: 通过本地集合模拟数据方式(Test测试时用.)
# 分区数规则: 默认是2个, 还可以通过 parallelize()函数设置分区数(最小分区数),  还可以通过 setMaster('local[N]')设置.

# 导包
from pyspark import SparkConf, SparkContext
import os

# 锁定远端Python版本.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# main方法
if __name__ == '__main__':
    # 1. 构建SparkContext对象.
    # 这里的3表示 3个线程(Task), 因为是通过 parallelize()函数获取的rdd, 所以现在能看到 3个分区.
    conf = SparkConf().setMaster('local[3]').setAppName('create_rdd_1')
    sc = SparkContext(conf = conf)

    # 2. 构建rdd对象,  这里的5, 表示: 5个分区.
    rdd_init = sc.parallelize(['乔峰', '虚竹', '段誉', '段延庆', '叶二娘', '岳老三', '云中鹤'], 5)

    # 3. 查看RDD中每个分区的数据, 以及分区的数量
    print(rdd_init.getNumPartitions())      # 获取分区数量, 默认: 2
    print(rdd_init.glom().collect())        # 获取每个分区的数据.


