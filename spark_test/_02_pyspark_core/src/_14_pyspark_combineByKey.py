# 目的: 演示下combineByKey的用法, 了解即可, 因为它太底层了, 一般我们用的套路是: reduceByKey() => foldByKey() => aggregateByKey() => combineByKey(fn1, fn2, fn3)

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
    print('PySpark模板')

    # 1. 创建Spark的核心对象
    conf = SparkConf().setAppName('wordCount').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 2. 初始化数据.
    rdd_init = sc.parallelize(
        [('c01', '张三'), ('c02', '李四'), ('c01', '王五'), ('c01', '赵六'), ('c02', '田七'), ('c03', '周八'), ('c02', '李九')])

    # 需求:
    """
        要求将数据转换为以下格式: 
            [
                ('c01',['张三','王五','赵六'])
                ('c02',['李四','田七','李九'])
                ('c03',['周八'])
            ]
    """

    '''
        格式:
          reduceByKey(0, fn1, fn1)
          foldByKey(0, fn1, fn2)
          aggregateByKey(初值, fn1, fn2)
          combineByKey(fn1, fn2, fn3)  
            参数1: fn1  设置初始值
			参数2: fn2  对每个分区执行函数
			参数3: fn3  对各个分区执行完结果汇总处理
    '''


    # 源数据为: [('c01', '张三'), ('c02', '李四'), ('c01', '王五'), ('c01', '赵六'), ('c02', '田七'), ('c03', '周八'), ('c02', '李九')].
    # 其实默认2个分区, 即:  [[('c01', '张三'), ('c02', '李四'), ('c01', '王五')], [('c01', '赵六'), ('c02', '田七'), ('c03', '周八'), ('c02', '李九')]]
    print(rdd_init.glom().collect())

    def fn1(agg):      # 设置初始值的, 假设处理: [('c01', '张三'), ('c02', '李四'), ('c01', '王五')] 这个分区, 我们返回:  ['张三']
        # print(f'fn1 {agg})')
        return [agg]   # 第1分区: c01的 ['张三']        c02的 ['李四']              第2分区: c01的 ['赵六']     c02的 ['田七']   c03的 ['周八']


    def fn2(agg, cur):      # 对每个分区执行函数, agg: 是fn1的结果(初值),  cur:就是当前分区, 遍历到的数据.
        agg.append(cur)     # 此处以第1分区举例: c01的 agg: ['张三'], cur:  '王五',    c02: agg:['李四'], cur: 空
        # print(f'fn2 {agg}')
        return agg          # 第1分区: ['c01':['张三', '王五'], 'c02':['李四']]       第2分区: ['c01':['赵六'], 'c02':['田七', '李九'], 'c03':['周八']]

    def fn3(agg, cur):      # 作用: 对各个分区执行完结果汇总处理 agg: 是fn1的结果(初值), cur: fn2执行后的结果(每个分区的数据)
        agg.extend(cur)     # 例如: agg: ['张三', '王五'], cur: ['赵六']  => ['张三', '王五', '赵六']
        return agg          # 返回: c01的 ['张三', '王五', '赵六']


    # 快速生成变量接收后续的值, 快捷键: ctrl + alt + v
    rdd_result = rdd_init.combineByKey(fn1, fn2, fn3)

    # 打印最终结果
    print(rdd_result.collect())
