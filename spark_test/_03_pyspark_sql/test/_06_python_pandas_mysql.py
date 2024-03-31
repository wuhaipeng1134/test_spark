# 导包
import pandas as pd
from sqlalchemy import create_engine

# main方法, 程序的主入口.
if __name__ == '__main__':
    print('目的: pandas写出数据到 mysql中')

    # 动作1: 因为要实现Pandas 和 MySQL的交互, 所有需要先安装驱动包.
    # 动作2: 把数据源文件上传到 data目录下.

    # 1. 读取1个外部的数据源.
    df = pd.read_csv('/export/data/workspace/bigdata60_parent/_03_pyspark_sql/data/tsv示例文件.tsv', sep='\t', index_col=[0])
    print(df)

    # 2. 构建连接数据库的对象.
    # # 构建连接数据库对象, 注意: day06_pyspark是数据库的名字, 你的数据库叫什么名字, 这里就写什么.
        # mysql 表示数据库类型
        # pymysql 表示python操作数据库的包
        # root: 表示数据库的账号和密码，用冒号连接
        # 127.0.0.1:3306/day08_pyspark 表示数据库的ip和端口，以及名叫test的数据库
        # charset=utf8 规定编码格式
    conn = create_engine('mysql+pymysql://root:123456@192.168.88.161:3306/day08_pyspark?charset=utf8')        # 获取连接对象.

    # 3. 把df的数据写入到数据库中.
    #         数据库名      连接对象.  不要索引         如果存在, 就追加.
    # df.to_sql('test_pdtosql', conn, index=False, if_exists='append')


    # 4. 从数据库中读取数据, 返回的是 Pandas的DataFrame对象.
    # 整张表
    # pd_sql2 = pd.read_sql('test_pdtosql', conn)
    # print(pd_sql2)

    # 指定列查询
    pd_sql3 = pd.read_sql('select birthday,name from test_pdtosql', conn)
    print(pd_sql3)