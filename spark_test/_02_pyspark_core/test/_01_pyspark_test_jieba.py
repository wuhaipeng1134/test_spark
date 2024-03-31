# 需求: 用来演示 jieba库, 如何进行分词的.

# 导包.
from pyspark import SparkConf, SparkContext
import os
import jieba        # 导包

# 目的: 用于解决 JAVA_HOME is not set 这个问题的, 其实你会发现, 这些代码不写也行.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 编写main方法, 快捷键, main + 回车
if __name__ == '__main__':
    print('测试 jieba库 进行分词. ')

    # 默认分词方案,  内容为: ['我要', '去', '黑马', '学习', '大', '数据', '专业']
    print(list(jieba.cut('我要去黑马学习大数据专业')))

    # 全模式(最细粒度分析), 内容为: ['我', '要', '去', '黑马', '学习', '大数', '数据', '专业']
    print(list(jieba.cut('我要去黑马学习大数据专业', cut_all=True)))

    # 搜索引擎模式, 内容为: ['我要', '去', '黑马', '学习', '大', '数据', '专业']
    print(list(jieba.cut_for_search('我要去黑马学习大数据专业')))




