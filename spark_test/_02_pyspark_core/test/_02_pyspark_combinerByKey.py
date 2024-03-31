#

# 导包.
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os
import time

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    # 1. 创建SparkSession对象.
    spark = SparkSession.builder.master('local[*]').appName('create_spark').getOrCreate()
    print(spark)

    # 2- 读取外部文件的数据
    df_init = spark.read.csv(
        path='file:///export/data/workspace/bigdata60_parent/_02_pyspark_core/data/stu.csv',
        header=True,
        sep=',',
        inferSchema=True
    )

    df_init.select()

    # 3- 获取数据 和 元数据信息
    df_init.show()
    df_init.printSchema()

    # 4- 需求: 将年龄大于20岁的数据获取出来 :  DSL
    df_where = df_init.where('age > 20')
    df_where.show()
    print('*' * 31)

    # 使用 SQL来实现
    df_init.createTempView('t1')

    spark.sql("select *  from  t1 where age > 20").show()

    # 关闭 spark对象
    spark.stop()
