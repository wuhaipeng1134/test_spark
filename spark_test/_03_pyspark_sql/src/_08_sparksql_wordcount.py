# 目的: 综合案例1: wordcount案例, 单词统计.   方式1: RDD(Spark Core) => DataFrame(Spark SQL)

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
    print('目的: wordcount案例, 单词统计')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

    # 获取 SparkContext对象.
    sc = spark.sparkContext

    # 2. 读取数据, 采用RDD方式.     格式: 'hello you Spark Flink'
    rdd = sc.textFile('file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/word.txt')

    # 3. 将数据转成一个个的单词.     格式: ['hello', 'you', 'Spark', 'Flink']  => [('hello', ), ('you', ), ('Spark', ), ('Flink', )]
    rdd_word = rdd.flatMap(lambda line: line.split()).map(lambda word: (word,))

    # 4. 把 RDD 转成 DF对象.
    df_words = rdd_word.toDF(schema=['words'])
    # 打印下结果, 我们看看
    # df_words.show()

    # 5. 具体的完成WordCount案例的代码.
    # 5.1  方式1: SQL方式.
    # 5.1.1 创建视图.
    df_words.createTempView('t1')
    # 5.1.2 通过SQL语句实现需求.
    spark.sql('select words, count(1) cnt from t1 group by words').show()

    # 5.2 方式2: DSL方式.
    df_words.groupBy('words').count().withColumnRenamed('count', 'cnt') .show()

    # 5.3 方式3: DSL方式 + SQL方式
    df_words.groupBy('words').agg(
        F.count('words').alias('cnt')       # 统计, 并给该列其别名.
    ).show()


    # 6. 关闭Spark
    spark.stop()

