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
    print('案例: 新零售案例, 数据清洗.')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('xls_clear').getOrCreate()

	# 2. 读取外部文件.
    df_init = spark.read.csv(path='file:///export/data/workspace/bigdata60_parent/_04_xls/data/E_Commerce_Data.csv',
                         header=True, inferSchema=True)

    # 清理数据的需求为:
    # print(f'清理前的数据总条数: {df_init.count()}')

    # 	需求一: 将客户id(CustomerID) 为 0的数据过滤掉
    # 	需求二: 将商品描述(Description) 为空的数据过滤掉
    # 3. 对数据进行清理动作.
    df_clear = df_init.where('CustomerID != 0 and Description is not null')
    # print(f'清理后的数据总条数: {df_clear.count()}')

    # 	需求三: 将日期格式进行转换处理, 例如: 原有数据信息: 12/1/2010 8:26  转换为: 2010-01-12 08:26
    # 思路: '12/1/2010 8:26'  => 整数类型的 秒值, 例如: 1236230123817 => 转成指定格式的字符串, 例如: '2010-01-12 08:26'
    # 函数解释: withColumn() 相当于新增一列, 如果列名和表中的某列列名重复, 就覆盖.
    # 函数解释: unix_timestamp() 根据格式, 将字符串日期转成对应的 秒值(整数形式).
    # 函数解释: from_unixtime()  将整数形式的秒值 根据格式转成对应的 日期字符串.
    df_clear = df_clear.withColumn(
        'InvoiceDate',
        F.from_unixtime(F.unix_timestamp(df_clear['InvoiceDate'], 'M/d/yyyy H:mm'), 'yyyy-MM-dd HH:mm:ss')
    )

    # 4. 将清理后的数据结果, 写到HDFS上.   /xls/output
    df_clear.write.mode('overwrite').csv(path='hdfs://node1:8020/xls/output', sep='\001')

    # 5. 关闭Spark对象.
    spark.stop()