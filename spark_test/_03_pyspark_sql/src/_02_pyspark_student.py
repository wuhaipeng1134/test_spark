# 目的: 演示下SparkSQL到底是如何编写的.

# 导包.
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

import os

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('PySpark模板')

    # 1. 创建Spark的核心对象, SparkSQL的 SparkSession对象.
    spark = SparkSession.builder.master('local[*]').appName('quickStart').getOrCreate()

    # 2. 读取外部文件中的数据.        DataFrame(SparkSQL的核心对象)
    df_init = spark.read.csv(
        path='file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/stu.csv',  # 数据源文件的目录
        header=True,  # 是否有头信息, 即: 源文件的第一行是否是列名.
        sep=', ',  # 行字段分隔符, 即: 按照什么格式切割整行数据.
        inferSchema=True  # 用于让程序自动推断字段的类型的, 默认是False(表示不推断, 即所有类型都是string类型)
    )
    # 3. 获取数据 和  元数据信息.
    df_init.show()  # 打印数据(列名, 数据)
    df_init.printSchema()  # 打印元数据, 例如: 字段, 类型等.
    print('*' * 30)

    # 4. 需求: 将年龄大于20的数据给获取出来, 我们用 DSL(DataSet的API)方式来做.
    df_where = df_init.where('age > 20')
    df_init.foreach()
    df_where.show()
    print('*' * 30)

    # 5. Spark SQL语句实现上述的需求.
    df_init.createTempView('t1')        # 基于DataFrame对象(的内容, 列名, 数据) 构建视图(临时表, 在内存中, 程序执行结束, 就销毁)
    spark.sql('select * from t1 where age > 20').show()

    # 6. 关闭程序.
    spark.stop()
