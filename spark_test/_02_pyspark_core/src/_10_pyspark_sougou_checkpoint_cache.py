# 目的: 演示检查点(CheckPoint) 和 缓存(Cache, 持久化)

# 导包.
import jieba
from pyspark import SparkConf, SparkContext, StorageLevel
import os
import time

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('搜狗数据分析')

    # 1. 创建Spark的核心对象
    conf = SparkConf().setAppName('souGou').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 4.1 设置检查点在HDFS上的存储位置. 虽然它也可以存储到本地Linux系统, 但是无意义(因为本地模式我们主要是测试用的, 测试时数据量一般不会特别大, 无意义)
    sc.setCheckpointDir('/spark/checkpoint')  # 默认路径就是HDFS路径, 所以可以省略 hdfs://node1:8020

    # 2. 读取外部文件.
    rdd_init = sc.textFile('file:///export/data/workspace/bigdata60_parent/_02_pyspark_core/data/SogouQ.sample')

    # 3. 写一些 rdd函数, 用于构成rdd的DAG图, 方便一会儿查看, 检查点的截断效果.
    # 以下的逻辑没有什么意义, 我就是给大家演示一下, 最终目的是: 看检查点的阶段效果.
    rdd_map1 = rdd_init.map(lambda line: line)
    rdd_map2 = rdd_map1.map(lambda line: line)
    rdd3 = rdd_map2.repartition(3)
    rdd_map3 = rdd3.map(lambda line: line)
    rdd_map4 = rdd_map3.map(lambda line: line)
    rdd4 = rdd_map4.repartition(2)
    rdd_map5 = rdd4.map(lambda line: line)

    # ---- 开启检查点 和 缓存 -----
    # 方式1: 先开启检查点, 然后开启缓存,  推荐.
    # 设置开启检查点
    rdd_map5.checkpoint()
    rdd_map5.persist(storageLevel=StorageLevel.MEMORY_AND_DISK).count() # 先开启检查点, 后开启缓存(推荐)

    # 或者
    # 方式2: 先开启缓存, 后开启检查点, 了解即可, 不推荐. 
    rdd_map5.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
    rdd_map5.checkpoint()
    rdd_map5.count()  # 先开启缓存, 后开启检查点, 可能会导致检查点失效, 这种方式理解即可, 一般不推荐.

    print(rdd_map5.take(10))

    time.sleep(1000)
