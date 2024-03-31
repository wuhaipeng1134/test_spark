# 导包
import pandas as pd

if __name__ == '__main__':
    print('目的: 演示pandas的两大核心对象之 Series, 它表示: 单列数据对象')

    # 1. 创建Series对象, 传入: 列表
    s1 = pd.Series(['刘亦菲', '赵丽颖', '高圆圆'])
    print(s1)

    # 1. 创建Series对象, 传入: 元组,   可以指定索引列(无论何种方式, 都支持), 但是索引的个数要和元素的个数保持一致.
    s2 = pd.Series(('刘亦菲', '赵丽颖', '高圆圆'), index=['A', 'B', 'C'])
    print(s2)

    # 1. 创建Series对象, 传入: 字典
    s3 = pd.Series({'name': '刘亦菲', 'age': 33, 'gender': '女'})
    print(s3)

    # 2. Series中的常用属性和方法
    print(s3[0])  # 刘亦菲
    print(s3.index)  # Index(['name', 'age', 'gender'], dtype='object')

    print(s3.shape) # (3,)   s3这个Series对象有几个值, 结果是 元组形式.
    print(s3.dtypes)    # data types 数据类型, object, 表示任意类型.
    print(s3.values)    # 获取所有的值,  ['刘亦菲' 33 '女']
    print('*' * 30)

    print(len(s3))  # 3
    print(s3.size)  # 3
    print(s3.head(2))   # 获取前两条数据, 默认是: 5条.
    print(s3.tail(2))   # 获取后两条数据, 默认是: 5条.
    print(s3.tolist())  # ['刘亦菲', 33, '女']
    print('*' * 30)

    # 演示运算相关.
    s4 = pd.Series(data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    s5 = pd.Series(data=[1, 2, 3, 4, 5, 6, 7, 8])

    print(s4.sum())     # 55, 求和
    print(s4.mean())    # 5.5, 求平均值
    print('*' * 30)

    # 参与计算
    print(s4 + s5)  # NaN  它的意思是: Not A Number, 即: 不是一个数字.
    print('*' * 30)

    # Series 转成 DataFrame
    print(s4.to_frame())        # 不带索引
    print(s4.reset_index())     # 带索引





