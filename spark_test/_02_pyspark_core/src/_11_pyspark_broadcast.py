# 目的: 演示 RDD共享变量之 广播变量(BroadCast Variables)

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
    print('广播变量案例')

    # 1. 创建Spark的核心对象
    conf = SparkConf().setAppName('wordCount').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 2. 设置广播变量
    # a = 10  # 如果是这种写法, 在RDD内部是, 每个Executor(进程)的每个线程(Task)都会 采用拷贝副本的方式, 把a拷贝到 该线程中.
    bc = sc.broadcast(10)       # 这种方式, 仅仅会把数据传给每个Executor, 然后由该Executor中的线程去Executor中拉取数据即可.

    # 3. 读取数据, 获取RDD对象.
    rdd_init = sc.parallelize(range(1, 11))

    # 4. 将每个数据的值都 + 指定的值, 后去新的RDD.
    # rdd_map = rdd_init.map(lambda num: num + a)
    rdd_map = rdd_init.map(lambda num: num + bc.value)      # bc.value的方式获取 广播变量的值.

    # 5. 打印上述RDD的值.
    # rdd_map.foreach(lambda num: print(num))
    print(rdd_map.collect())
