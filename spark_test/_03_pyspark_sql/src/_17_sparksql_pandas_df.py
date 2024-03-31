# 导包.
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pandas as pd
import os

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('讲解 pandas的DataFrame对象 和 Spark的DataFrame对象如何相互转换, 目的: 为后续讲解Pandas的UDF, UDAF函数做铺垫.')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

    # 格式1:  spark_df = spark.crateDataFrame(pandas的DataFrame对象)         把pandas的DataFrame对象 转成 spark的DataFrame对象.
    # 格式2:  pd_df = spark_df.toPandas()                                   把spark的DataFrame对象 转成 pandas的DataFrame对象.

    # 2. 创建pandas的DF对象.
    pd_df = pd.DataFrame({'name': ['刘亦菲', '赵丽颖', '高圆圆'], 'age': [33, 31, 35]})

    # 3. 可以用Pandas的API来对数据进行 分析处理的动作.
    # 过滤出age>32的
    pd_df = pd_df[pd_df['age'] > 32]
    print(pd_df)

    # 4. 将其(Pandas的DataFrame对象) 转成  SPark DF
    spark_df = spark.createDataFrame(pd_df)

    # 5. 可以调用Spark DataFrame中的方法了.
    # 对age求和.
    spark_df.select(F.sum('age').alias('age_sum')).show()

    # 6. 把 Spark DataFrame 转成 Pandas DataFrame.
    pd_df = spark_df.toPandas()
    print(pd_df)
