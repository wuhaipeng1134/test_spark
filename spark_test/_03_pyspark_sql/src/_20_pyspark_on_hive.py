# 导包.
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('目的: 演示通过代码连接: Spark On Hive')

    # 1. 创建SparkSQL的核心对象 SparkSession
    # spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()      # 连接的还是Spark自己本地的库.

    # Spark On Hive的方式连接, 即: 通过Spark代码, 操作的是Hive的数据库
    # 参数 spark.sql.shuffle.partitions 解释:  SparkSQL的 分区数, 默认: 200
    # 参数 spark.sql.warehouse.dir      解释:  指定默认加载数据的路径地址, 在Spark中, 如果不设置, 默认是本地路径. 这里写的是Hive库表在HDFS上的存储路径.
    # 参数 hive.metastore.uris          解释:  指定metastore的服务地址.
    # 函数 enableHiveSupport()          解释:  开启 和 Hive的集成
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('SparkOnHive_1') \
        .config('spark.sql.shuffle.partitions', '4') \
        .config('spark.sql.warehouse.dir', 'hdfs://node1:8020/user/hive/warehouse') \
        .config('hive.metastore.uris', 'thrift://node1:9083') \
        .enableHiveSupport() \
        .getOrCreate()

    # 2. 执行SQL语句.
    spark.sql('show databases').show()
