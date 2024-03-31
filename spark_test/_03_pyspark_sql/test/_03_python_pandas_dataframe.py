# 导包
import pandas as pd

if __name__ == '__main__':
    print('目的: 演示pandas的两大核心对象之 DataFrame, 它表示: 整个数据对象, 即二维表, 相当于多列(Series)组成')

    # 1. 构建DataFrame对象, 字典方式, 里边的1个元素是 1列数据.
    df = pd.DataFrame(data={
        'name': ['刘亦菲', '高圆圆', '赵丽颖'],  # 第一列, 即: 第1个Series对象
        'age': [33, 35, 31],  # 第二列, 即: 第2个Series对象
        'address': ['北京', '上海', '深圳']  # 第三列, 即: 第3个Series对象
    })
    print(df)

    # 2. 构建DataFrame对象, 列表方式, 里边的1个元素是 1行数据.
    df = pd.DataFrame(data=[
        ['刘亦菲', 33, '北京'], ['高圆圆', 35, '上海'], ['赵丽颖', 31, '深圳']
    ])
    print(df)

    # 3. 构建DataFrame对象, 列表 + 元组
    df = pd.DataFrame(data=[
        ('刘亦菲', 33, '北京'), ('高圆圆', 35, '上海'), ('赵丽颖', 31, '深圳')
    ], index=['A', 'B', 'C'])
    print(df)

    # 4. 构建DataFrame对象, 元组
    df = pd.DataFrame(data=(
        ('刘亦菲', 33, '北京'), ('高圆圆', 35, '上海'), ('赵丽颖', 31, '深圳')
    ), index=['A', 'B', 'C'])
    print(df)
    print('-' * 30)

    # 5. 演示常用API
    print(len(df))      # 3, 统计长度, 即: 多少行.
    print(df.size)      # 9, 统计长度, 即: 多少个元素,  行数 * 列数
    print('-' * 30)

    # 遍历
    for col_name in df:         # col_name 即: df对象中的 列名
        for value in df[col_name]:  # df[列名]  根据列名, 获取该列的值.
            print(value)
    print('-' * 30)

    # 执行一些相关运算.
    print(df + df)
    print('-' * 30)

    print(df[1] + df[1])    # 这里的1是列名的意思, 类似于: name, age等这些.
