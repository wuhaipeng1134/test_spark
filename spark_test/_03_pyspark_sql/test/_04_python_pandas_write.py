# 导包
import pandas as pd

# main方法, 程序的主入口.
if __name__ == '__main__':
    print('目的: pandas写出数据到 文件中. ')

    # 1. 读取1个外部的数据源.
    df = pd.read_csv('/export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/1960-2019全球GDP数据.csv',
                      encoding='gbk')

    # 2. 对数据进行处理, 获取所有 country值为 中国的信息.
    df_china = df[df['country'] == '中国']
    print(df_china)

    # 3. 把处理后的数据(即: df_china)写到1个文件中.
    df_china.to_csv('/export/spark_data/china.csv', index=False, header=False, sep='|')   # 记得去Linux本地把这个路径创建出来.