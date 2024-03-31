# 案例2: 点击流日志数据分析.

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
    print('点击流日志数据分析')

    # 1. 创建Spark的核心对象
    conf = SparkConf().setAppName('click_demo').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 2. 读取外部文件的数据.
    rdd_init = sc.textFile('file:///export/data/workspace/bigdata60_parent/_02_pyspark_core/data/access.log')

    # 3. 过滤出 非空行数据 且 切割后 字段个数 >= 12的数据.
    rdd_filter = rdd_init.filter(lambda line : line.strip() != '' and len(line.split()) >= 12)

    # 4. 根据需求, 完成指定的代码编写.
    # 4.1: 统计pv(访问次数) 和 uv(用户数量)
    # 4.1.1 统计PV, 访问次数.
    print(rdd_filter.count())

    # 4.1.2 统计UV, 用户数量.             ip地址           去重
    print(rdd_filter.map(lambda line: line.split()[0]).distinct().count())

    # 4.2: 统计每个访问的URL的次数, 找到前10个
    #                                    (url, 1)
    # print(rdd_filter.map(lambda line: (line.split()[6], 1)).reduceByKey(lambda agg, cur: agg + cur).top(10))  不建议用top方式.
    print(rdd_filter.map(lambda line: (line.split()[6], 1)).reduceByKey(lambda agg, cur: agg + cur).sortBy(lambda res: res[1], False).take(10)) # 推荐使用sortBy方式

    rdd_filter.cache()


