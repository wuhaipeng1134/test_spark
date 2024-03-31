# 导包.
import pandas as pd
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
    print('目的: 演示通过Pandas来定义 SparkSQL的UDF函数(一进一出), 普通函数')
    print('自定义SparkSQL函数的步骤: 1.写个Python函数.  2.注册(3种方式).  3.使用')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

	# 2. 构建数据集.
    df_init = spark.createDataFrame([(1, 3), (2, 5), (3, 8), (5, 4), (6, 7)], schema='a int,b int')
    # df_init.show()  打印数据
    df_init.createTempView('t1')

    # 3. 处理数据, 需求: 计算 a 和 b的乘积
    # 3.1 定义Python函数.
    # 3.2.2 语法糖写法, 仅支持 DSL风格.
    @F.pandas_udf(returnType=IntegerType())
    # def pd_cj(a, b):        # 简单写法, 由系统自己加数据类型.
    def pd_cj(a:pd.Series, b:pd.Series) -> pd.Series:          # 完整写法.   pd.Series 表示Pandas表中的一列数据.
        return a * b

    # 3.2 注册Python函数为 SparkSQL函数.
    # 3.2.1 标准注册方式, 适用于: DSL 和 SQL风格.
    # 給DSL用的                     给SQL风格用的
    pd_cj_dsl = spark.udf.register('pd_cj_sql', pd_cj)

    # 3.3 使用.
    # SQL风格
    spark.sql('''
        select a, b, pd_cj_sql(a, b) cj from t1
    ''').show()

    # DSL风格.
    df_init.select('a', 'b', pd_cj_dsl('a', 'b').alias('cj')).show()

    df_init.select('a', 'b', pd_cj('a', 'b').alias('cj')).show()

