# 演示PySpark入门案例, WordCount
# 需求: 从HDFS上读取数据, 对数据进行统计分析, 最后讲结果根据 单词数量 进行降序排列, 并将结果写到HDFS上.

# 导包.
from pyspark import SparkConf, SparkContext
import os

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# main方法
if __name__ == '__main__':
    print('PySpark案例: 从HDFS上读取数据')

    # 1. 创建Spark对象.
    # conf = SparkConf().setMaster('local[*]').setAppName('wd_hdfs')   # 把当前的PySpark程序 提交到 SparkLocal环境执行.

    conf = SparkConf().setMaster('yarn').setAppName('wd_hdfs')   # 把当前的PySpark程序 提交到 Yarn平台执行.
    # conf = SparkConf().setAppName('wd_hdfs')   # 上述的代码, 还可以写入如下的样子. 则 spark-submit 提交的时候, 如果写了 --master yarn 也会提交到yarn平台.
    sc = SparkContext(conf=conf)

    # 2. 从HDFS上读取数据.    file:/// 本地(Linux系统)    hdfs://node1:8020/  HDFS目录
    rdd_init = sc.textFile('hdfs://node1:8020/data/A.txt')

    # 3. 对数据进行切割, 获取到每一个单词.
    rdd_flatMap = rdd_init.flatMap(lambda line: line.split())       # ['hello', 'spark', 'world', 'hello']

    # 4. 把数据转成 (单词,1)的形式.
    rdd_map = rdd_flatMap.map(lambda word: (word, 1))   # [('hello', 1), ('spark', 1), ('world', 1), ('hello', 1)]

    # 5. 根据单词分组, 统计每个单词的次数, sum: 用来记录单词次数的累加结果的, cur:单词当 前的次数.
    rdd_result = rdd_map.reduceByKey(lambda sum, cur: sum + cur)
    # 打印处理后的数据.
    print(f'排序前: {rdd_result.collect()}')   # [('world', 4), ('hadoop', 10), ('hive', 9), ('hello', 10), ('sqoop', 1)]

    # 6. 对结果数据进行排序, 要求: 根据 单词数量 进行降序排列
    # sortBy()函数: 我们可以自定义排序字段 和 规则的.
    # sortByKey()函数: 默认是按照key排序的, 可以设定规则(升序, 降序)
    # top()函数: 可以自定义排序字段, 但是不能设置 规则(升序, 降序), 默认是降序的.

    # 方式1: 正常的排序逻辑, 实际开发也是这样写的.
    # 参数解释: wd_tup, 单词元组的意思, 格式是:  ('world', 4)     wd_tup[1]: 根据单词总数进行排序     ascending=False 表示降序.
    rdd_sort = rdd_result.sortBy(lambda wd_tup : wd_tup[1], ascending=False)
    print(f'排序后: {rdd_sort.collect()}')     # [('hadoop', 10), ('hello', 10), ('hive', 9), ('world', 4), ('sqoop', 1)]

    # 方式2: 采用数据转换的方式, 也是可以实现类似于排序的需求, 只不多代码稍多, 这个只是为了演示 函数, 并不是让你掌握这种排序, 实际开发也不用.
    # 思路:  ('hadoop', 10) => (10, 'hadoop'), 排序 =>  ('hadoop', 10)
    # rdd_result = rdd_result.map(lambda wd_tup : (wd_tup[1], wd_tup[0]))
    # rdd_sort = rdd_result.sortByKey(ascending=False)
    # rdd_sort = rdd_sort.map(lambda wd_tup : (wd_tup[1], wd_tup[0]))
    # print(f'排序后: {rdd_sort.collect()}')

    # 方式3: top(N)函数实现排序, 只能进行降序排列, 且写入数字是几, 最终就获取几个, 可能存在没有获取到全部元素的情况.
    # top()函数是直接触发的, 不需要通过 collect()方式获取数据.
    # rdd_top = rdd_result.top(3, lambda wd_tup : wd_tup[1])
    # print(f'排序后: {rdd_top}')

    # 7. 把数据写出到HDFS上, 要求: 父目录必须存在, 子目录必须不存在.    即: HDFS的 /data目录必须存在,   /data/output 目录必须不存在.
    # 我们发现数据写到两个文件了, 说明: PySpark默认有2个分区.
    rdd_sort.saveAsTextFile('hdfs://node1:8020/data/output')
    print('数据写入HDFS成功!')

    # 8. 释放资源.
    sc.stop()