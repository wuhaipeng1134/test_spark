# 演示PySpark入门案例, WordCount

# 导包.
from pyspark import SparkConf, SparkContext
import os
import time
import jieba

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    # 1. 创建Spark的核心对象, SparkContext(Spark的上下文对象, 承上启下作用)
    # 我们当前这个Spark程序的名字叫: WordCount,  master:表示占用的资源,  local[*]表示可以使用所有的 可用资源.
    conf = SparkConf().setAppName('wordCount').setMaster('local[*]')
    sc = SparkContext(conf=conf)



    # 2. 读取数据.
    # 小细节, 这里再读取数据的时候, 路径最好不要这样写, 可能会出问题, 建议加上 文件协议.    本地环境: file:///   HDFS环境: hdfs://node1:8020/
    # rdd_init = sc.textFile('../data/A.txt')      # 逐行读取, 整行数据格式为: "hello world hello hadoop"

    # 读取本地环境下的 words.txt文件, 这里的路径是 windows环境下的路径, 注意: 读取不出来, 因为这里我们说的 "本地环境"指的是 Linux环境.
    # rdd_init = sc.textFile('file:///D:/教案/WorkClass/BigData60_BJ/day01_pyspark/资料/A.txt')      # 逐行读取, 整行数据格式为: "hello world hello hadoop"

    # 读取本地环境下(Linux环境)下的文件, 这次是可以的.
    rdd_init = sc.textFile('file:////export/data/workspace/bigdata60_parent/_01_pyspark_base/data/words.txt')      # 逐行读取, 整行数据格式为: "hello world hello hadoop"

    # 打印读取到的内容, 用于测试是否读取成功.
    # print(rdd_init.collect())     #  数据格式: ['hello world hello hadoop', 'hadoop hello world hive', 'hive hive hadoop',]

    # 3. 对每一行的数据执行切割操作, 转换为一个一个的列表, 我们通过 map()函数实现, 注意: 这里就是给你看看打不到我们要的效果.
    # rdd_map = rdd_init.map(lambda line : line.split())
    # print(rdd_map.collect())       # [['hello', 'world', 'hello', 'hadoop'], ['hadoop', 'hello', 'world', 'hive'], ['hive', 'hive', 'hadoop']]

    # 针对于上述的情况, 最终我们要采用 flatMap函数实现, 它 = flatten(扁平化) + map(映射, 理解为转换)
    rdd_flatmap = rdd_init.flatMap(lambda line : line.split())
    # print(rdd_flatmap.collect())        # ['hello', 'world', 'hello', 'hadoop', 'hadoop', 'hello', 'world', 'hive']

    # 4. 把每个单词转成: (单词, 1)
    rdd_map = rdd_flatmap.map(lambda word : (word, 1))
    # print(rdd_map.collect())             # [('hello', 1), ('world', 1), ('hello', 1), ('hadoop', 1), ('hadoop', 1)]

    # 5. 根据key(单词)进行分组聚合统计操作, 函数可以设置聚合方案,  sum: 用于记录局部累加结果的.   cur表示该单词的次数.
    rdd_result = rdd_map.reduceByKey(lambda sum, cur: sum + cur)

    # 打印最终结果
    print(rdd_result.collect())

    time.sleep(100)     # 单位: 秒. 休眠100秒.

    sc.stop()   # 释放资源.

   #  rdd_result.repartition()