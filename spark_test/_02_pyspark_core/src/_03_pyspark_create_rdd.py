# 演示如何创建RDD对象.
# 方式2: 通过读取外部文件的方式方式(生产环境使用)

# 情境2: 读取外部文件之  多个文件.

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
    conf = SparkConf().setMaster('local[3]').setAppName('create_rdd_3')
    sc = SparkContext(conf = conf)

    # 2. 构建RDD对象.   本地路径(Linux路径) file:///    HDFS路径  hdfs://node1:8020/
    # 如果写的不是文件路径, 而是文件夹路径, 说明把该文件夹(目录)下所有的(文件)数据都读取了
    # 这里的5的意思是: 至少5个分区.
    rdd_init = sc.textFile('file:///export/data/workspace/bigdata60_parent/_02_pyspark_core/data', 1)

    # 3. 查看RDD中每个分区的数据, 以及分区的数量
    print(rdd_init.getNumPartitions())  # 获取分区数量, 默认: 4,  因为有 4个小文件, A.txt, B.txt, C.txt, words.txt
    print(rdd_init.glom().collect())    # 获取每个分区的数据.

    # 遇到的问题, 当读取后, 分区过多, 在写入到HDFS的时候, 就会产生大量的小文件.
    # 如果由于源数据文件较多导致的这个问题, 可以采用在读取(文件)的时候, 尽量减少分区数.

    # 解决方案: 可以采用 读取的时候, 合并小文件的方式实现, 尽量减少分区数.