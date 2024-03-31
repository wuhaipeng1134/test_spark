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
    print('目的: 演示SparkSQL写出数据到普通文件')

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

    # 4. 把清洗后的数据写到文件中.
    '''
        SparkSQL写出数据到文件中的统一格式:
            Spark从文件中读取数据的格式.
                df.read.format('text/csv/json/...).option(K, V).load(数据源文件的路径)       
            
            Spark写出数据到文件的格式.
                df.write.mode().format('text/csv/json/parquet/orc/hive...').option(K, V).save(目的地文件的路径)
                
                mode: 传入模式, 字符串可选: append追加, overwrite覆盖, ignore忽略, error重复就报异常(默认的)
                format: 传入格式, 字符串, 可选: text, csv, json, parquet, orc, avro, jdbc
                    //注意: text源只支持单列df写出
                option: 设置属性, 例如: option('sep', ', ')
                save: 	写出的路径, 支持本地文件和HDFS    
    '''

    # 写出到文件中, csv格式
    # df.write.mode('overwrite').format('csv').option('sep', '|').option('header', True).save('hdfs://node1:8020/data/output_csv')

    # 写出到文件中, json格式
    # df.write.mode('overwrite').format('json').save('hdfs://node1:8020/data/output_json')

    # 写出到文件中, text格式, 注意: text源只支持单列df写出
    # df.select('name').write.mode('overwrite').format('text').save('hdfs://node1:8020/data/output_text')

    # 写出到文件中, 列式存储(Parquet, Orc)
    df.write.mode('overwrite').format('Parquet').save('hdfs://node1:8020/data/output_parquet')

    # 写出到文件中, hive文件.  目前暂时无法演示, 因为我们还没有做 Spark 和 Hive的整合.
    # df.write.mode('overwrite|append|ignore|error').saveAsTable('表名', '存储类型')
