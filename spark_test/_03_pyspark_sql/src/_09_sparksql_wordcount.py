# 目的:  综合案例1: wordcount案例, 单词统计.   方式2: 读取外部文件, 获取DataFrame(Spark SQL), 然后处理.

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
    print('目的: ')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

    # 2. 读取数据, 直接通过 SparkSession对象, 读取源文件即可.
    df_init = spark.read.text('file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/word.txt')

    # 3. 处理数据, 把数据转成1一列, 一行1个单词的情况.
    # df_words = df_init.select(F.split('value', ' ').alias('words'))   # 每行都是1个数组, [hello, you, ...]
    df_words = df_init.select(
        # 炸裂函数     切割整行数据   切割符   起别名
        F.explode(F.split('value', ' ')).alias('words')
    )  # 每行都是1个单词

    # 4. 完成需求.
    # 4.1 方式1: DSL方式.
    df_words.groupBy('words').count().show()

    # 4.2 方式2: DSL方式 + SQL
    df_init.createTempView('t1')
    df_words_new = spark.sql('select explode(split(value, " ")) words from t1')   # SQL写法
    df_words_new.groupBy('words').count().show()     # DSL写法

    # 4.3 方式3: 纯SQL语句
    # df_init.createTempView('t1')
    df_words_sql = spark.sql('select explode(split(value, " ")) words from t1')
    # 创建视图, 存储上述的结果.
    df_words_sql.createTempView('t2')    # 一行1个单词
    # 最终SQL实现
    spark.sql('select words, count(1) cnt from t2 group by words').show()

    # 上述 纯SQL写法, 可以变形为: 如下的写法.
    # 不用侧视图
    spark.sql('''
        select 
            words, count(1) cnt 
        from 
            (select explode(split(value, " ")) words from t1) as t2 
        group by words
    ''').show()

    # 用侧视图
    # 格式解释: 表1  侧视图(存储炸裂函数结果的)   炸裂函数    侧视图名   侧视图的中的列名
    # 代码:    t1 lateral view explode(split(value, " ")) t2 as words
    spark.sql('''
        select 
            words, count(1) cnt 
        from 
            t1 lateral view explode(split(value, " ")) t2 as words  
        group by words
    ''').show()

    # 5. 关闭spark程序
    spark.stop()
