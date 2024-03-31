# 演示如何创建RDD对象.
# 方式2: 通过读取外部文件的方式方式(生产环境使用)

# 情境1: 读取外部文件之  1个文件.

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
    # 这里的3表示 3个线程(Task)
    conf = SparkConf().setMaster('local[3]').setAppName('create_rdd_2')
    sc = SparkContext(conf = conf)

    # 2. 构建RDD对象.   本地路径(Linux路径) file:///    HDFS路径  hdfs://node1:8020/
    # 这里的5的意思是: 至少5个分区.
    rdd_init = sc.textFile('file:///export/data/workspace/bigdata60_parent/_02_pyspark_core/data/words.txt', 5)

    # 3. 查看RDD中每个分区的数据, 以及分区的数量
    print(rdd_init.getNumPartitions())  # 获取分区数量, 默认: 2
    print(rdd_init.glom().collect())    # 获取每个分区的数据.