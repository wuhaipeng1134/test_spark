# 目的: 演示累加器, 注意: 目前只有 加 的功能.

# 导包.
from pyspark import SparkConf, SparkContext
import os
import time

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('PySpark模板')

    # 1. 创建Spark的核心对象
    conf = SparkConf().setAppName('wordCount').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 注意: 广播变量是只读的, 各个线程只能读取数据, 不能修改数据.
    # 版本2: 演示使用 广播变量的情况.
    # 定义1个变量, 广播变量.
    a = sc.accumulator(10)

    # 2. 获取rdd对象.
    rdd_init = sc.parallelize(range(1, 11))  # 1 ~ 10
    print(rdd_init.glom().collect())


    # 3. 处理数据, 把列表中的数据, 都和a累加到一起.
    def fn1(num):
        # 通过累加器的方式, 实现增加.
        a.add(num)  # 这里的add()就是累加器的意思, 用于修改 广播变量的值的, 只能修改值, 不能查询值.
        # a.value         # 这样写会报错的. 对于线程来说, 只能增加广播变量的值, 不能进行读取, 读取的操作, 只能由Driver来处理.
        # 报错信息: Accumulator.value cannot be accessed inside tasks   Accumulator.value 不能在任务内部访问
        return num


    # 4. 具体的执行流程.
    rdd_map = rdd_init.map(fn1)

    rdd_map.cache().count()  # 设置缓存, 最终结果65, 如果不设置, 最终结果是: 10 + 55 + 55 = 120

    print(rdd_map.collect())

    rdd_2 = rdd_map.map(lambda num: num + 1)
    print(rdd_2.top(10))  # 我们发现, 下述再打印累加器的结果时, 它的值就变成了: 120, 说明累加器又执行了一次.

    print(a.value)  # 获取累加器的结果,  65
