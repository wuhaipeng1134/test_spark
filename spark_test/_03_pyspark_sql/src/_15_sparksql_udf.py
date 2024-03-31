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
    print('目的: 演示通过Python代码自定义UDF函数, 返回结果: 字符串类型(基本类型)')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2. 读取外部数据集.
    df_init = spark.read.csv(path='file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/student.csv',
                         schema='id int, name string, age int')
    # 打印结果
    # df_init.show()

    # 把上述的df结果创建为视图
    df_init.createTempView('t1')

    # 3. 处理数据, 即: 通过自定义UDF函数, 实现 在所有name的后边添加 _itcast.
    # 即: zhangsan(处理前的数据) => zhangsan_itcast(处理后的数据)

    # 3.1 写1个Python函数, 实现:  给接收到的变量(name)后加1个  _itcast
    # 3.2.3 方式3: 在函数的上边直接注册, 注意: 它不能和 方式2共用, 你得把方式2注释或者删除了.
    @F.udf(returnType=StringType())
    def concat_udf(name: str) -> str:       # 细节: 参数类型, 及方法返回值类型 str可以省略不写.
        return name + "_itcast"

    # 3.2 把上述的函数注册为 Spark函数(UDF, 一进一出)
    # 3.2.1 方式1的格式: udf对象 = sparkSession.udf.register(参数1, 参数2,参数3)
    # 参数1: udf函数的函数名称, 此名称用于在SQL风格中使用, 参数2: 需要将哪个python函数注册为udf函数, 参数3: 设置python函数返回的类型
    # 在DSL风格中用的                     在SQL风格中用的.      python函数  返回值类型
    # concat_udf_dsl = spark.udf.register('concat_udf_sql', concat_udf, StringType())

    # 3.2.2 方式2: 仅适用于 DSL方案, 格式: udf对象 =F.udf(参数1,参数2)
    # 参数1: 需要将哪个python函数注册为udf函数, 参数2: 设置python函数返回的类型
    # concat_udf_dsl2 = F.udf(concat_udf, StringType())


    # 3.3 使用上述已经注册后的Spark UDF函数.
    # 3.3.1 SQL风格.
    # spark.sql('select id, concat_udf_sql(name) as name, age from t1').show()    # 正确
    # spark.sql('select id, concat_udf_dsl(name) as name, age from t1').show()      # 报错, concat_udf_dsl是UDF对象, 是给DSL用的.

    # 3.3.2 DSL风格.
    # 测试方式1
    # df_init.select('id', concat_udf_dsl('name').alias('name'), 'age').show()     # 正确
    # df_init.select('id', concat_udf_sql('name').alias('name'), 'age').show()     # 报错

    # 测试方式2
    # df_init.select('id', concat_udf_dsl2('name').alias('name'), 'age').show()     # 正确

    # 测试方式3            Python函数名和Spark函数一样了.
    df_init.select('id', concat_udf('name').alias('name'), 'age').show()     # 正确
