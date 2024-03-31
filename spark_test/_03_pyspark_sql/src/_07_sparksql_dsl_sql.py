# 目的: 演示DataFrame的两种编程风格, 即:  DSL风格(就是DataFrame支持的API),   SQL风格(就是写SQL语句)

'''
    DSL解释:	 特定领域语言
		在Spark SQL中 DSL指的 dataFrame的相关的API, 这些API相当简单, 因为大多数的API都与SQL的关键词同名

	SQL:
		主要通过编写SQL 完成统计分析操作

	思考: 在工作中, 这两种方案, 会使用那种呢?
		1. 在刚刚接触使用Spark SQL的时候, 更多的使用的 SQL的方式来操作 (大多数的行为)
		2. 从Spark官方角度来看, 推荐使用DSL, 因为DSL对于框架更容易解析处理, 效率相对SQL来说 更高效一些 (本质差别不大)
		3. 一般编写DSL看起来要比SQL高大上一些, 而且当你习惯DSL方案后, 你会不断尝试使用的

	DSL相关的API:
		show(参数1, 参数2):
			用于显示表中相关数据, 参数1表示控制展示多少条, 默认是20,  参数2: 是否截断列, 默认只输出20个字符的长度, 过长不显示.
			一般都不设置相关的参数, 直接用

		printSchema():      用于打印表的结构信息(元数据信息)

		select():
			//此API是用于实现 在SQL中 select后面放置的内容的
			//比如说: 可以放置字段, 或者相关函数, 或者表达式
			问: 如何查看每个API支持传递那些方式呢?
			答: 看源码, 例如: def select(self, *cols)	//这里的 *cols表示可变参数, 然后去看 Parameters参数介绍, 能看到它支持: 字符串, 列表, column对象

			在使用dataFrame的相关API的时候, 传入参数的说明:
				在使用dataFrame的API的时候, 每个API的传入的参数方式都支持多种方式: 字符串, 列表, column对象

			select如何使用呢?
				字符串方式:
					df.select('id,name,age')
					df.select(df['id'],df['name'])
				列表方式:
					df.select(['id','name','age'])

				column对象:
					df.select([df['id'],df['name'],df['age']])
					df.select(['id',df['name'],'age'])

		filter 和 where: 	用于对数据进行过滤的操作

		groupBy: 	        对数据执行分组, 说明: 分组必聚合, 不然没有意义.

	注意:
		如果想在DSL中使用SQL的函数, 在spark SQL中, 专门将函数放置在一个类中, 具体如下:
		1. 先导入函数的核心对象:
			import pyspark.sql.functions as F

		2. 使用F.函数名 即可使用, 例如:  F.expr(...)
			相关spark支持的所有的SQL函数: https://spark.apache.org/docs/3.1.2/api/sql/index.html


    SQL编程风格介绍:
        注意事项:
            如果使用SQL方式来处理, 必须将DF注册为一个视图(可以理解为: 临时表, 程序执行结束, 就没有了): 支持临时视图 和 全局视图.
        例如:
            df.createTempView('视图名')            注册1个临时视图
            df.createGlobalTempView('视图名')      注册1个全局视图
            df.createOrReplaceTempView('视图名')   注册1个临时视图, 如果存在进行替换.
        格式:
            spark.sql('编写SQL语句')
'''

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
    print('目的: 演示DataFrame的两种编程风格, 即: DSL, SQL')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()

	# 2. 读取数据, 获取 DataFrame对象.
    df = spark.read.csv(path='file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/stu2.csv',
                         header=True, sep=',', inferSchema=True)

    # 3. 演示DSL风格, 即: DataFrame的各种API
    # 打印内容.
    df.show()

    # 打印元数据
    df.printSchema()

    # select操作, 查询id, address的信息
    df.select('id', 'address').show()           # 传入字符串
    df.select(['id', 'address']).show()         # 传入字符串列表

    df.select(df['id'], df['address']).show()   # 传入列对象(StructField)
    df.select([df['id'], df['address']]).show()   # 传入列对象(StructField)的列表

    # where: 条件判断 filter: 过滤
    df.where('age > 20').show()
    df.where(df['age'] > 20).show()

    df.filter('age > 20').show()
    df.filter(df['age'] > 20).show()

    # groupBy(), 统计每个地区的总人数.
    # 纯DSL语句, 即: DataFrame自身的API
    df.groupBy('address').count().show()

    # DSL语句 +  SQL语句
    # groupBy(), 统计每个地区的总人数 及 多少个不同年龄的人(大白话: 年龄去重, 统计总数)
    df.groupBy('address').agg(
        F.count('id').alias('cnt'),     # 统计各地区的总人数
        # 细节: 如果要在DSL中, 使用SQL的函数, 则必须先导包: import pyspark.sql.functions as F , 然后通过 F.函数名()的方式调用.
        F.countDistinct('age').alias('age_cnt')   # alias: 起别名的    结论:  多少个不同年龄的人
    ).show()

    # 4. 演示SQL风格, 即: 执行SQL语句.
    # 创建视图
    df.createTempView('t1')
    # 执行查询即可.
    spark.sql('select address, count(1) cnt, count(distinct age) age_cnt from t1 group by address').show()
