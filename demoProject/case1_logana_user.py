import  jieba
from pyspark import SparkContext, SparkConf,StorageLevel
from  wordprocess import process,userprocess
if __name__ == '__main__':
    conf = SparkConf().setAppName('demo2').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    rdd = sc.textFile('../data/SogouQ.txt')
    rdd1 = rdd.map(lambda x: x.split('\t'))
    rdd2 = rdd1.map(lambda x:(x[1],x[2]))
    rdd3 = rdd2.flatMap(userprocess)

    rdd4 = rdd3.map(lambda x: (x,1))
    print(rdd4.reduceByKey(lambda a, b: a + b).sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(500))

   # rdd3.persist(StorageLevel.DISK_ONLY)
   # print(rdd3.take(5))
    #rdd4 = rdd3.filter(lambda x: x not in ['谷', '学院','客'])
   # rdd5 = rdd4.map(lambda x: (x,1))
   # rdd6 = rdd5.reduceByKey(lambda a,b: a+b).sortBy(lambda x :x[1],ascending=False,numPartitions=1)
    #print(rdd6.take(10))
