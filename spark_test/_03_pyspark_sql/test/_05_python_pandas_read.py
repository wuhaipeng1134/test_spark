# 导包
import pandas as pd

# main方法, 程序的主入口.
if __name__ == '__main__':
    print('目的: pandas从文件中读取数据. ')

    # 1. 读取1个外部的数据源.

    # 直接读取, year列的值的格式是: 年份
    # df = pd.read_csv('/export/spark_data/china.csv', sep='|', header=None, names=['year', 'country', 'GDP'])

    # 直接读取, 对year列的值做简单的修改, 格式为: XXXX-XX-XX
    df = pd.read_csv('/export/spark_data/china.csv',
                     sep='|', header=None,
                     names=['year', 'country', 'GDP'],
                     parse_dates=['year']   # 用来把year列的值(年份), 转成对应的 日期格式, 即: 1961-01-01
    )

    # 2. 对数据进行处理, 这里我们只是为了演示Pandas的读取数据功能的, 直接打印即可.
    print(df)
