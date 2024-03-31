# 目的: SparkSQL_构建DataFrame对象     方式1: RDD转DF

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
    print('方式1: 构建DataFrame对象, RDD转DF')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

    # 从SparkSession对象中, 直接构建出 SparkContext对象.
    sc = spark.sparkContext

    # 2. 读取数据, 获取 RDD对象
    rdd_init = sc.parallelize(
        [('c01', '张三'), ('c02', '李四'), ('c01', '王五'), ('c01', '赵六'), ('c02', '田七'), ('c03', '周八'), ('c02', '李九')])

    # 3. 通过RDD将 c01过滤掉, 没啥意义, 就是为了演示用的.
    rdd_filter = rdd_init.filter(lambda tup: tup[0] != 'c01')
    # print(rdd_filter.collect())

    # 4. 基于RDD, 将其转成对应的 DataFrame对象.
    # 4.0 构建出 SparkSQL表对象(即: DataFrame对象的 元数据信息, 即: 多少列, 列名, 列的类型, 是否非空)
    # 细节, 千万别忘记加 ()
    schema = StructType().add('id', StringType(), True).add('name', StringType(), True)

    # 4.1 方式1                   源数据       元数据
    df1 = spark.createDataFrame(rdd_filter, schema)
    df1.show()
    df1.printSchema()
    print('*' * 30)

    # 4.2 方式2
    df2 = spark.createDataFrame(rdd_filter, schema=['id', 'name'])
    df2.show()
    df2.printSchema()
    print('*' * 30)

    # 4.3 方式3
    df3 = spark.createDataFrame(rdd_filter)
    df3.show()
    df3.printSchema()
    print('*' * 30)

    # 4.4 方式4
    df4 = rdd_filter.toDF()
    df4.show()
    df4.printSchema()
    print('*' * 30)

    # 4.5 方式5
    df5 = rdd_filter.toDF(schema)
    df5.show()
    df5.printSchema()
    print('*' * 30)

    # 4.6 方式6
    df6 = rdd_filter.toDF(schema=['id', 'name'])
    df6.show()
    df6.printSchema()

