# 导包.
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import Window as win
import os

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('目的: 演示窗口函数的用法. ')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('windows').getOrCreate()

	# 2. 读取外部数据源.
    df_init = spark.read.csv(path='file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/pv.csv', header=True,
                         inferSchema=True)

    # df_init.show()

    # 3. 演示窗口函数, 按照uid分组, 按照pv降序排列, 有序号.
    #  把上述的结果转成临时视图.
    df_init.createTempView('pv_t1')

    # SQL语句
    spark.sql('''
        select 
            uid, datestr, pv,
            row_number() over (partition by uid order by pv desc) r1,
            rank() over (partition by uid order by pv desc) r2,
            dense_rank() over (partition by uid order by pv desc) r3
        from pv_t1
    ''').show()

    # DSL写法. 使用之前, 一定要先导包.  from pyspark.sql import Window as win
    df_init.select(
        df_init['uid'],
        df_init['datestr'],
        df_init['pv'],
        F.row_number().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('r1'),
        F.rank().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('r2'),
        F.dense_rank().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('r3')
    ).show()

    # 下边这个写法也可以.
    # df_init.select(
    #    'uid',
    #    'datestr',
    #    'pv'
    # ).show()
