# 目的: 演示如何获取SparkSession对象, 它是SparkSQL程序的入口对象,  就类似于: SparkContext是SparkCore的入口对象一样.

# 导包.
from pyspark import SparkConf, SparkContext
import os
from pyspark.sql import SparkSession

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('PySpark模板')

    # 1. 创建SparkSQL的核心对象: SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_SparkSession').getOrCreate()

    # 2. 打印对象.
    print(spark)



