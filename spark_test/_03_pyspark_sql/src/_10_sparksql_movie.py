# 目的: 综合案例, 电影数据分析.

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


def getAvgByUser():
    # SQL实现.
    spark.sql('''
        select 
            userid, round(avg(score), 2) u_avg 
        from 
            t1 
        group by userid order by u_avg desc limit 10
    ''').show()

    # DSL实现 + SQL语句
    df_init.groupBy('userid').agg(
        F.round(F.avg('score'), 2).alias('u_avg')  # SQL函数
    ).orderBy(F.desc('u_avg')).limit(10).show()


# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('目的: 读取外部文件, 获取DataFrame对象, 进行数据分析(分析 电影数据)')

    # 1. 创建SparkSQL的核心对象 SparkSession
    # 手动通过代码方式设置 4个分区.                                                          手动设置shuffle分区数.
    spark = SparkSession.builder.master('local[*]').appName('create_df').config('spark.sql.shuffle.partitions', 4).getOrCreate()

    # 我们没有指定分区, 默认 200个分区.
    # spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

    # 2. 读取 movie数据集(字段: userid 用户id , movieid 电影id,score 电影评分,datestr 评分时间)
    df_init = spark.read.csv('hdfs://node1:8020/spark/data/u.data', sep='\t',
                             schema='userid string, movieid string,score int,datestr string')

    # 查看下DataFrame对象的结果.
    # df_init.show()  # 默认只展示20行数据.
    # df_init.printSchema()

    # 3. 处理数据.
    df_init.createTempView('t1')  # 先根据DataFrame对象, 获取其对应的视图写法.

    # 3.1 需求1:查询用户平均分(即: 统计每个用户打分平均分), 按照平均分降序排列, 只获取前Top10
    # getAvgByUser()

    # 3.2 需求4: 需求: 查询高分电影中(语句: 分数大于3分的为高分电影) 打分次数最多的用户, 并求出此人打的平均分.
    # 思路: 1. 找出所有的高分电影.   2. 在高分电影中, 找到打分次数最多的用户.  3.计算该用户在所有电影中打的平均分.

    # 方式1: SQL语句实现.
    # 3.2.1.找出所有的高分电影.
    df_top_movie = spark.sql("""
        select 
            movieid, round(avg(score), 2) avg_score 
        from 
            t1 
        group by movieid having avg_score > 3;
    """)
    df_top_movie.createTempView('t2_m_top')  # 列: movieid(电影id), 该(高分)电影的平均分(avg_score)

    # 3.2.2.在高分电影中, 找到打分次数最多的用户.  # userid: 655
    df_u_top = spark.sql("""
        select 
            userid
        from 
            t1 join t2_m_top 
        on t1.movieid = t2_m_top.movieid
        group by t1.userid order by count(1) desc limit 1
    """)
    df_u_top.createTempView('t3_u_top')

    # 3.2.3.计算该用户在所有电影中打的平均分.
    # join方式查询.
    spark.sql("""
        select 
            t1.userid, round(avg(t1.score), 2) avg_score
        from 
            t1 join t3_u_top on t1.userid = t3_u_top.userid
        group by t1.userid
    """).show()

    # 子查询方式.
    spark.sql("""
        select 
            userid, round(avg(score), 2) avg_score
        from 
            t1 
        where userid in (
             select 
                t1.userid
            from 
                t1 join t2_m_top 
            on t1.movieid = t2_m_top.movieid
            group by t1.userid order by count(1) desc limit 1
        ) 
        group by userid
    """).show()

    # 方式2: DSL方式实现.
    # 3.2.4.找出所有的高分电影.
    #  df_top_movie = spark.sql("""
    #     select
    #         movieid, round(avg(score), 2) avg_score
    #     from
    #         t1
    #     group by movieid having avg_score > 3;
    # """)

    # 参考上述的SQL, 写 DSL代码即可.
    df_top_movie = df_init.groupby('movieid').agg(
        F.round(F.avg('score'), 2).alias('avg_score')
    ).where('avg_score > 3')

    # 3.2.5.在高分电影中, 找到打分次数最多的用户.
    #  df_u_top = spark.sql("""
    #     select
    #         userid
    #     from
    #         t1 join t2_m_top
    #     on t1.movieid = t2_m_top.movieid
    #     group by t1.userid order by count(1) desc limit 1
    # """)

    # 参考上述的SQL, 写 DSL代码即可.
    df_u_top =df_top_movie.join(df_init, 'movieid').groupBy('userid').agg(
        F.count('userid').alias('u_cnt')
    ).orderBy(F.desc('u_cnt')).select('userid').limit(1)

    # 3.2.6.计算该用户在所有电影中打的平均分.
    # join方式查询.
    # spark.sql("""
    #        select
    #            t1.userid, round(avg(t1.score), 2) avg_score
    #        from
    #            t1 join t3_u_top on t1.userid = t3_u_top.userid
    #        group by t1.userid
    #    """).show()

    # 参考上述的SQL, 写 DSL代码即可.
    df_init.join(df_u_top, 'userid').groupBy('userid').agg(
        F.round(F.avg('score'), 2).alias('avg_score')
    ).show()


    # 子查询方式.
    # spark.sql("""
    #        select
    #            userid, round(avg(score), 2) avg_score
    #        from
    #            t1
    #        where userid in (
    #             select
    #                t1.userid
    #            from
    #                t1 join t2_m_top
    #            on t1.movieid = t2_m_top.movieid
    #            group by t1.userid order by count(1) desc limit 1
    #        )
    #        group by userid
    #    """).show()

    # 参考上述的SQL, 写 DSL代码即可.
    # df_u_top.first()['userid']   df_u_top(DataFrame对象,即: 二维表), first: 获取第一行数据.   [列名]: 获取对应列的数据值.
    df_init.where(df_init['userid'] == df_u_top.first()['userid']).groupby('userid').agg(
        F.round(F.avg('score'), 2).alias('avg_score')
    ).show()