from pyspark import SparkContext, SparkConf,StorageLevel
if __name__ == '__main__':
    conf = SparkConf().setAppName('case2').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3])
    tmp = [3,2,1]
    bd = sc.broadcast(tmp)

    rdd1 = rdd.map(lambda x:x+bd.value[1])
    print(rdd1.collect())