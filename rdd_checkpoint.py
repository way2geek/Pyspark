import time

from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("sss2")
    sc = SparkContext(conf=conf)
    sc.setCheckpointDir('/Users/xhh/Rddpresist')
    rdd = sc.parallelize(range(1, 100), 3)
    def process(iter):
        l = []
        for i in iter:
            l.append(i+10)
        return l
    rdd2 = rdd.mapPartitions(process)
    rdd2.checkpoint()
    #rdd2 = rdd2.cache()
    rdd2.collect()
    rdd4 = rdd2.map(lambda a :  a+10)
    print(rdd4.collect())
    time.sleep(10000000)