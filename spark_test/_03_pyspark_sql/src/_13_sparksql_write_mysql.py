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
    print('目的: 演示SparkSQL写出数据到MySQL中.')

    # 1. 创建SparkSQL的核心对象 SparkSession, SparkSQL默认分区200个.
    spark = SparkSession.builder.master('local[*]').appName('create_df').config('spark.sql.shuffle.partitions', 1).getOrCreate()

    # 2. 读取数据集.
    df_init = spark.read.csv(path='file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/stu.csv',
                             sep=',',
                             header=True, inferSchema=True)

    # df_init.show()

    # 3. 对数据进行清洗的操作.
    df = df_init.dropDuplicates()  # 去重
    df = df.dropna()  # 清除null数据.

    # df.show()

    # 4. 把清洗后的数据写到MySQL中.
    '''
        SparkSQL写出数据到文件中的统一格式:
            Spark从文件中读取数据的格式.
                df.read.format('text/csv/json/...).option(K, V).load(数据源文件的路径)       
            
            Spark写出数据到文件的格式.
                df.write.mode().format('text/csv/json/parquet/orc/hive/jdbc...').option(K, V).save(目的地文件的路径)
                
                mode: 传入模式, 字符串可选: append追加, overwrite覆盖, ignore忽略, error重复就报异常(默认的)
                format: 传入格式, 字符串, 可选: text, csv, json, parquet, orc, avro, jdbc
                    //注意: text源只支持单列df写出
                option: 设置属性, 例如: option('sep', ', ')
                save: 	写出的路径, 支持本地文件和HDFS    
    '''

    # 写出到mysql中,  jdbc: Java DataBase Connectivity(Java数据库连接, 即: Java操作数据库的技术,  类似于之前学过的PyMysql)
    # 细节1: 我们写出到的是 Linux系统中的MySQL数据库, 不是window本地的MySQL数据库.
    # 细节2: 我们在写出数据到数据库的时候, 需要提前把数据库造出来, 但是表可以不用创建, 由程序自动帮我们创建.
    # 建库的实例代码如下: create database day08_pyspark charset 'utf8';
    df.write.mode('overwrite').format('jdbc')\
        .option('url', 'jdbc:mysql://node1:3306/day08_pyspark?useSSL=false&useUnicode=true&characterEncoding=utf-8')\
        .option('dbtable', 'stu')\
        .option('user', 'root')\
        .option('password', '123456')\
        .save()

    '''
        如果提示: java.sql.SQLException: No suitable driver, 是缺少驱动, 把 mysql驱动(在今日资料下) 拷贝到指定的地方即可.   
            1. python环境, 即: anaconda下.      node1, node2, node3都要做. 
                如果你用的是anaconda自带的base沙箱, 路径是:   /root/anaconda3/lib/python3.8/site-packages/pyspark/jars
                是你自己创建的沙箱, 路径是: /root/anaconda3/envs/pyspark_env/lib/python3.8/site-packages/pyspark/jars 
            2. spark环境,  用 spark-submit 提交到local的时候要配, 因为spark就在node1装了, 所以就node1配置即可.
            3. 在HDFS的/spark/jars目录下配置,  用 spark-submit 提交到yarn的时候要配.
            
        但是对于通用的jar包, 建议三个地方都配置.  
    '''