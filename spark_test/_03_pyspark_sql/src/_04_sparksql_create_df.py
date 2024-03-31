# 目的: SparkSQL_构建DataFrame对象     方式2: pandas的 DataFrame => Spark DataFrame
# pandas: Python中专门用于做数据分析的库, 里边有很多的API 以及 接口, 只能处理单机(本地)的数据, 相对于SparkSQL来说, 只能处理少量规模的数据.

# 导包.
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import pandas as pd

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('目的: 演示如何将 Pandas DataFrame => Spark DataFrame')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

	# 2. 构建 pandas_DF对象(记得先导包).
    pandas_df = pd.DataFrame({'id': ['c01', 'c02', 'c03'], 'name': ['刘亦菲', '赵丽颖', '高圆圆']})
    print(pandas_df)
    print('-' * 30)      # 就是一个分割线.

    # 3. 把 pandas_DF对象 =>  Spark DF对象.
    spark_df = spark.createDataFrame(pandas_df)

    # 4. 打印结果.
    spark_df.show()             # 打印表中的数据(列名, 数据)
    spark_df.printSchema()      # 打印表的元数据.

# 问题: 这种方式有什么用?
# 答案: 最终目的是扩充Spark的读取的数据源, 当处理的数据源比较特殊的时候, 例如: Excel, 就可以通过Pandas的DF直接读取,
# 然后将其转成Spark的DF来处理即可. 