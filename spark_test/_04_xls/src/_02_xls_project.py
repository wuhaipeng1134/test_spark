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


def xuqiu_1():
    # SQL风格.
    spark.sql('''
        select 
            Country,
            count(distinct CustomerID) cnt_cid
        from 
            xls_t1
        group by Country order by cnt_cid desc limit 10
    ''').show()
    # DSL风格.
    df_init.groupby('Country').agg(
        F.countDistinct('CustomerID').alias('cnt_cid')
    ).orderBy('cnt_cid', ascending=False).limit(10).show()

    df_init.groupby('Country').agg(
        F.countDistinct('CustomerID').alias('cnt_cid')
    ).orderBy(F.desc('cnt_cid')).limit(10).show()


def xuqiu_3():
    spark.sql('''
            select 
                Country,
                round(sum(Quantity * UnitPrice), 2) total_money
            from 
                xls_t1 where InvoiceNo not like 'C%'
            group by Country order by total_money desc
    ''').show()
    # DSL风格.
    df_init.where("InvoiceNo not like 'C%'").groupby('Country').agg(
        F.round(F.sum(df_init['Quantity'] * df_init['UnitPrice']), 2).alias('total_money')
    ).orderBy('total_money', ascending=False).show()


def xuqiu_5():
    # SQL风格.
    spark.sql('''
        select
            words,
            count(1) as cnt_words
        from
            xls_t1 lateral view explode(split(Description, ' ')) t2 as words
        group by words order by cnt_words desc limit 300
    ''').show()
    # DSL风格.
    df_init.withColumn('words', F.explode(F.split('Description', ' '))).groupby('words').agg(
        F.count('words').alias('cnt_words')
    ).orderBy('cnt_words', ascending=False).limit(300).show()


def xuqiu_9():
    # SQL风格.
    spark.sql('''
        select
            Country,
            count(distinct InvoiceNo) cnt_oid, 
            count(distinct if(InvoiceNo like 'C%', InvoiceNo, Null)) c_cnt_oid 
        from
            xls_t1
        group by Country order by cnt_oid desc
    ''').show()
    # DSL风格.
    df_init.groupby('Country').agg(
        F.countDistinct('InvoiceNo').alias('cnt_oid'),
        F.countDistinct(F.expr("if(InvoiceNo like 'C%', InvoiceNo, Null)")).alias('c_cnt_oid'),
    ).orderBy(F.desc('cnt_oid')).show()


# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('案例: 演示新零售项目的需求, 10个需求. ')

    # 1. 创建SparkSQL的核心对象 SparkSession
    spark = SparkSession.builder.master('local[*]').appName('xls_project').getOrCreate()

    # 2. 读取数据.
    # 2.1 定义Schema对象(元数据: 列名, 类型)
    # 字段解释: 订单编号(退货订单以C 开头), 产品代码, 产品描述, 购买数量(负数表示退货), 订单日期和时间, 商品单价, 客户编号, 国家名字
    schema = StructType() \
        .add('InvoiceNo', StringType()) \
        .add('StockCode', StringType()) \
        .add('Description', StringType()) \
        .add('Quantity', IntegerType()) \
        .add('InvoiceDate', StringType()) \
        .add('UnitPrice', DoubleType()) \
        .add('CustomerID', IntegerType()) \
        .add('Country', StringType())

    # 2.2 具体的读取处理后数据源的动作.
    df_init = spark.read.csv(path='hdfs://node1:8020/xls/output', sep='\001', schema=schema)

    # 2.3 把上述的结果, 放到临时视图中.
    df_init.createTempView('xls_t1')

    # 3. 对数据进行处理.
    # 3.1 客户数最多的10个国家
    # xuqiu_1()

    # 3.2 销量最高的10个国家

    # 3.3 各个国家的总销售额分布情况, 除去退款的.
    # SQL风格.
    # xuqiu_3()

    # 3.4 销量最高的10个商品

    # 3.5 商品描述的热门关键词Top300
    # 细节, 关键词的格式: 'CERAMIC HEART FAIRY CAKE MONEY BANK', 我们需要通过 explode()给它炸开, 然后用 lateral view(侧视图)来临时保存它的结果.
    # 之后再用侧视图 和 原表xls_t1做关联查询即可.
    # xuqiu_5()

    # 3.6 退货订单数最多的10个国家
    # 3.7 月销售额随时间的变化趋势
    # 3.8 日销量随时间的变化趋势

    # 3.9 各国的购买订单量和退货订单量的关系
    # xuqiu_9()

    # 3.10 商品的平均单价与销量的关系
