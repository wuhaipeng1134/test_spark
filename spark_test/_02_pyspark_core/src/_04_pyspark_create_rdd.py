# 演示如何创建RDD对象.
# 方式2: 通过读取外部文件的方式方式(生产环境使用)

# 情境3: 读取外部文件之  多个文件, 并对小文件做合并, 尽量减少分区数.

# 导包
from pyspark import SparkConf, SparkContext
import os

# 锁定远端Python版本.
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# main方法
if __name__ == '__main__':
    # 1. 构建SparkContext对象.
    # 这里的3表示 3个线程(Task)
    conf = SparkConf().setMaster('local[3]').setAppName('create_rdd_4')
    sc = SparkContext(conf = conf)

    # 2. 构建RDD对象.   本地路径(Linux路径) file:///    HDFS路径  hdfs://node1:8020/
    # 如果写的不是文件路径, 而是文件夹路径, 说明把该文件夹(目录)下所有的(文件)数据都读取了
    # 这里的5的意思是: 至少5个分区.
    # wholeTextFiles()的作用: 尽可能的减少分区数量, 从而减少最终输出到目的地文件的数量.
    rdd_init = sc.wholeTextFiles('file:///export/data/workspace/bigdata60_parent/_02_pyspark_core/data', 5)

    # 3. 查看RDD中每个分区的数据, 以及分区的数量
    print(rdd_init.getNumPartitions())  # 获取分区数量, 默认: 2,  [[(文件路径, 文件内容), (文件路径, 文件内容)], [(文件路径, 文件内容), (文件路径, 文件内容)]]
    print(rdd_init.glom().collect())    # 获取每个分区的数据.

#分区后,数据格式如下.
'''
[
	# 第一个分区的数据.
	[
		# 第1个文件
		(
			'file:/export/data/workspace/bigdata60_parent/_02_pyspark_core/data/words.txt', 
			
			'hello world hello hadoop\r\nhadoop hello world hive\r\nhive hive hadoop\r\nhadoop hadoop hive\r\nhive hadoop hello hello\r\nsqoop hive hadoop hello hello\r\nhello world hello hadoop\r\nhadoop hello world hive\r\nhive hive hadoop'
		), 
		
		# 第2个文件
		(
			'file:/export/data/workspace/bigdata60_parent/_02_pyspark_core/data/A.txt',
			
			'AAAAAAAAAAAAAAAAAAAA\r\nhello world hello hadoop\r\nhadoop hello world hive\r\nhive hive hadoop\r\nhadoop hadoop hive\r\nhive hadoop hello hello\r\nsqoop hive hadoop hello hello\r\nhello world hello hadoop\r\nhadoop hello world hive\r\nhive hive hadoop'
		), 
		
		
		# 第3个文件
		(
			'file:/export/data/workspace/bigdata60_parent/_02_pyspark_core/data/B.txt', 
			
			'BBBBBBBBBBBBBB\r\nhello world hello hadoop\r\nhadoop hello world hive\r\nhive hive hadoop\r\nhadoop hadoop hive\r\nhive hadoop hello hello\r\nsqoop hive hadoop hello hello\r\nhello world hello hadoop\r\nhadoop hello world hive\r\nhive hive hadoop'
		)
	], 
	
	
	# 第二个分区的数据
	[
		# 第1个文件
		(
			'file:/export/data/workspace/bigdata60_parent/_02_pyspark_core/data/C.txt', 
			
			'CCCCCCCCCCCCCCCCCC\r\nhello world hello hadoop\r\nhadoop hello world hive\r\nhive hive hadoop\r\nhadoop hadoop hive\r\nhive hadoop hello hello\r\nsqoop hive hadoop hello hello\r\nhello world hello hadoop\r\nhadoop hello world hive\r\nhive hive hadoop'
		)
	]
]
'''