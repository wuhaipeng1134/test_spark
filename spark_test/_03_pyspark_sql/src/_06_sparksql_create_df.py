# 目的: SparkSQL_构建DataFrame对象     方式3: 读取外部文件, 例如: text, csv, json, parquet...

# 导包.
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('目的: ')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

	# 2. 读取数据.
    # 方式1: "标准"写法.
    df1 = spark.read.format('json').load(
        'file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/people.json')

    # 输出结果.
    df1.show()
    df1.printSchema()
    print('-' * 30)

    # 方式2: "简写"形式.
    df2 = spark.read.json('file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/people.json')

    # 输出结果.
    df2.show()
    df2.printSchema()


