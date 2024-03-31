
# 导包
import pandas as pd

if __name__ == '__main__':
    print('目的: 演示pandas入门')

    # pandas是Python的一个第三方库, 专门做结构化数据的处理的, 只能处理单机数据, 不能分布式处理, 一般用于做数据清洗用居多, 俗称: 潘大师.

    # 1. 基于pandas读取数据, 返回的是 Pandas的 DataFrame对象
    # df = pd.read_csv('../data/1960-2019全球GDP数据.csv', encoding='gbk')
    # df = pd.read_csv('/export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/1960-2019全球GDP数据.csv', encoding='gbk')
    df = pd.read_csv('file:///export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/1960-2019全球GDP数据.csv', encoding='gbk')

    # 打印df对象, 查看结果.
    # print(df)

    # 2. 做一些简单的查询, 其实还是 pandas中 DataFrame对象的函数.
    # print(df.head())        # 默认5行.
    # print(df.head(3))       # 获取3行数据

    # 3. 简单需求: 获取country为中国的前10条数据.
    china_df = df[df.country == '中国']
    print(china_df.head(10))

    # 上述写法的简写形式.
    print(df[df.country == '中国'].head(10))

