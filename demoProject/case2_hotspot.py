from pyspark import SparkContext, SparkConf,StorageLevel
from  wordprocess import process,userprocess,timeprocess
if __name__ == '__main__':
    conf = SparkConf().setAppName('case2').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    rdd = sc.textFile('../data/SogouQ.txt')
    rdd_time = rdd.map(lambda x:x.split('\t'))  # split to get elements
    rdd2 = rdd_time.flatMap(lambda x:timeprocess(x[0]))
    rdd3 = rdd2.map(lambda x:(x,1))
    print(rdd3.reduceByKey(lambda a, b: a + b). \
          sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(10))

