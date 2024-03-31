# 案例1: 搜狗数据分析.

# 导包.
import jieba
from pyspark import SparkConf, SparkContext, StorageLevel
import os
import time

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 需求1: 统计每个关键词出现了多少次
def keywordCounts():
    # 5.1.1 获取搜索词
    rdd_search = rdd_map.map(lambda line_tup: line_tup[2])  # 类似于: 我要去黑马学习大数据专业
    # 5.1.2 对搜索词进行分词操作, 获取到 关键词.
    rdd_flatMap = rdd_search.flatMap(lambda search: jieba.cut(search))
    # 5.1.3 把关键词 转成 (关键词, 1), 并进行分组统计.
    rdd_result = rdd_flatMap.map(lambda keyword: (keyword, 1)).reduceByKey(
        lambda agg, cur: agg + cur)  # [('传智', 6), ('heima', 10), ('大数据', 3)]
    # 5.1.4 对结果数据进行排序(倒序)
    rdd_sort = rdd_result.sortBy(lambda res: res[1], False)
    # 5.1.5 获取结果, 为了方便查看, 我们就获取 前 50条.
    print(rdd_sort.take(50))

# 需求2: 统计每个用户每个搜索词点击的次数
def userSearchCounts():
    # 5.2.1 提取 用户 和 搜索词数据.
    rdd_user_search = rdd_map.map(lambda line_tupe: (line_tupe[1], line_tupe[2]))  # (2982199073774412, '360安全卫士')
    # 5.2.2 基于用户 和 搜索词进行分组统计即可.
    # ((2982199073774412, '360安全卫士'), 1)
    rdd_result = rdd_user_search.map(lambda user_search: (user_search, 1)).reduceByKey(lambda agg, cur: agg + cur)
    # 5.2.3 对结果数据进行排序.
    rdd_sort = rdd_result.sortBy(lambda res: res[1], False)
    # 5.2.4 打印结果数据的前30条数据.
    print(rdd_sort.take(30))


# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('搜狗数据分析')

    # 1. 创建Spark的核心对象
    conf = SparkConf().setAppName('souGou').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 2. 读取外部文件.
    rdd_init = sc.textFile('file:///export/data/workspace/bigdata60_parent/_02_pyspark_core/data/SogouQ.sample')

    # 3. 过滤出合法数据.  不为空, 且 按照 空格或者\t切割后, 字段的数量必须是 6个.
    # 整行数据格式: 00:00:00	2982199073774412	[360安全卫士]	8 3	download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html
    rdd_filter = rdd_init.filter(lambda line: line.strip() != '' and len(line.split()) == 6)

    # 4. 对数据进行切割, 将数据放置到一个元组中: 一行数据(6个字段) 放到1个元组中.
    rdd_map = rdd_filter.map(lambda line: (
        line.split()[0],
        line.split()[1],
        line.split()[2][1:-1],  # 去掉搜索词两端的中括号, 即: 把 [360安全卫士] => 360安全卫士   它其实是搜索内容, 不是关键字.
        line.split()[3],
        line.split()[4],
        line.split()[5]
    ))
    # 打印下数据看看.
    print(rdd_map.take(10))


    # 5. 进行统计分析操作.
    # 5.1. 统计每个关键词出现了多少次,                 SQL语句: select 关键词, count(1) from 表名 group by 关键词;
    keywordCounts()

    # rdd_map.unpersist().count()

    # 5.2. 统计每个用户每个搜索词点击的次数             SQL语句: select 用户, 搜索词, count(1) from 表名 group by 用户, 搜索词;
    userSearchCounts()

    # 5.3. 统计每个小时点击次数	  # 作业, 自己写.       SQL语句: select 时间(小时), count(1) from 表名 group by 时间(小时);

    time.sleep(1000)