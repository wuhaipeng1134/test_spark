# 导包.
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import os

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    # 自定义UDF函数的步骤:  1.写1个Python函数.  2.把它注册为Spark的函数.  3.在SparkSQL中使用它.
    print('目的: 演示通过Python代码自定义UDF函数, 返回: 列表或者字典. ')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2. 读取外部数据集.
    df_init = spark.read.csv(path='file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/user.csv',
                             header=True, inferSchema=True)
    # 打印结果
    df_init.show()

    # 把上述的df结果创建为视图
    df_init.createTempView('t1')


    # 3. 处理数据, 即: 通过自定义UDF函数, 实现 将line字段切割, 将其转成: 姓名, 地址, 年龄.

    # 3.1 写1个Python函数, 实现:  给接收到的变量(name)后加1个  _itcast
    def split_3col_1(line):  # 返回列表
        return line.split('|')  # 返回结果: [张三, 北京, 20]


    def split_3col_2(line):  # 返回字典
        arr = line.split('|')  # 返回结果: [张三, 北京, 20]
        return {'name': arr[0], 'address': arr[1], 'age': arr[2]}  # 返回结果: {name:张三, address:北京, age:20}


    # 3.2 把上述的函数注册为 Spark函数(UDF, 一进一出)
    # 3.2.1.1 我们创建Schema对象(表示SparkSQL表的元数据)
    schema = StructType().add('name', StringType()).add('address', StringType()).add('age', StringType())

    # 3.2.1.2 具体的注册动作.
    # 3.2.1 方式1的格式: udf对象 = sparkSession.udf.register(参数1, 参数2,参数3)
    split_3col_1_dsl = spark.udf.register('split_3col_1_sql', split_3col_1, schema)


    # 3.2.2 方式2: 仅适用于 DSL方案, 格式: udf对象 =F.udf(参数1,参数2)
    split_3col_2_dsl = F.udf(split_3col_1, schema)

    # 3.2.3 方式3: 在函数上直接注册, 仅适用于 DSL语句.
    # 略.

    # 3.3 使用上述已经注册后的Spark UDF函数.
    # 3.3.1 SQL风格.
    spark.sql('''
        select 
            userid, 
            split_3col_1_sql(line) 3col, 
            split_3col_1_sql(line)['name'] name,
            split_3col_1_sql(line)['address'] address,
            split_3col_1_sql(line)['age'] age
        from t1
    ''').show()

    # 3.3.2 DSL风格.
    df_init.select(
        'userid',
        split_3col_2_dsl('line').alias('3col'),
        split_3col_2_dsl('line')['name'].alias('name'),
        split_3col_2_dsl('line')['address'].alias('address'),
        split_3col_2_dsl('line')['age'].alias('age')
    ).show()
