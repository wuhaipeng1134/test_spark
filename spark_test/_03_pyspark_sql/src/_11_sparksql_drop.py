# 目的: 演示Spark SQL中 数据清洗相关的API, 主要讲解: dropDuplicates() 去重, dropNa() 删除null值, fillNa() 替换null值

# 导包.
from mailcap import subst

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
    print('目的: 演示Spark SQL中 数据清洗相关的API')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

	# 2. 读取数据, 获取DataFrame对象.
    df_init = spark.read.csv(path='file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/stu.csv', sep=',', inferSchema=True, header=True)

    # 数据源数据中的 清洗前的数据.
    # df_init.show()

    # 3. 演示 SparkSQL的 数据清洗相关的API.
    # 演示 dropDuplicates(): 去重API, 默认情况下: 只有整行数据都一样, 我们才认为是重复数据, 只保留重复数据的第一行, 其它行删掉.
    # df_init.dropDuplicates().show()

    #如果 name列 和 address列相同, 我们认为是重复数据, 就把它删掉.
    # df_init.dropDuplicates(['name', 'address']).show()


    # dropna() 删除null数据的,
    # df_init.dropna().show()   # 默认情况: 该行只要有一列为null, 该行会被整体删除.
    # df_init.dropna(thresh=2).show()    # 该行只要有2列有效数据(即: 不为null), 否则删除该行数据.

    # 限定 该行的 age, name, address列, 三列中至少有2列有合法数据.
    # df_init.dropna(thresh=2, subset=['age', 'name', 'address']).show()

    # fillNa() 替换null值
    # 用指定的值, 填充所有列的 null值.
    # df_init.fillna('hg').show()    # 因为默认值是字符串, 只会填充所有string类型的列.
    # df_init.fillna(123).show()     # 因为默认值是整数, 只会填充所有integer类型的列.

    # 把name列的null值用 hg替换
    # df_init.fillna('hg', subset=['name']).show()

    # 设置各列的替换规则.
    df_init.fillna({'id': 0, 'name':'刘亦菲', 'age': 100}).show()
