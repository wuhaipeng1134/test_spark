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
    print('案例: 在线教育项目数据分析, 2个需求.')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

	# 2. 读取数据.
    df_init = spark.read.csv(path='file:///export/data/workspace/bigdata60_parent/_05_edu/data/eduxxx.csv', sep='\t',
                         header=True, inferSchema=True)

    # 根据上述读取到的数据, 创建临时视图.
    df_init.createTempView('edu_t1')

    # 3. 处理数据.
    # 需求一: 找到TOP50热点题对应科目. 然后统计这些科目中, 分别包含几道热点题目
    # 3.1 SQL风格.
    # 3.1.1 找到TOP50热点题
    df_top50 = spark.sql('''
        select
            question_id,
            sum(score) total_question
        from
            edu_t1
        group by question_id order by total_question desc limit 50
    ''')
    # 创建视图, 存储上述的查询结果.
    df_top50.createTempView('top50_q')

    # 3.1.2 根据上述的热点题信息, 找到它们对应的科目, 统计这些科目中, 分别包含几道热点题目
    spark.sql('''
        select
            edu_t1.subject_id,
            count(distinct top50_q.question_id) sub_cnt
        from
            edu_t1 join top50_q 
        on 
            edu_t1.question_id = top50_q.question_id
        group by edu_t1.subject_id
        
    ''').show()


    # 3.2 DSL风格.
    # 3.2.1 找到TOP50热点题
    df_top50_dsl = df_init.groupby('question_id').agg(
        F.sum('score').alias('total_question')
    ).orderBy('total_question', ascending=False).limit(50)

    # 3.2.2 根据上述的热点题信息, 找到它们对应的科目, 统计这些科目中, 分别包含几道热点题目
    df_top50_dsl.join(df_init, 'question_id').groupby('subject_id').agg(
        F.countDistinct(df_top50_dsl['question_id']).alias('sub_cnt')
    ).show()


    # 需求二:  找到Top20热点题对应的推荐题目. 然后找到推荐题目对应的科目, 并统计每个科目分别包含推荐题目的条数
    # 自己尝试写一下.