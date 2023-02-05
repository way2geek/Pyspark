from pyspark import  SparkConf,SparkContext
if __name__ == '__main__':
    sf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=sf)
    rdd = sc.parallelize([1,23,4,4,5,3,5,523,4,52,34,4])
    print(rdd.getNumPartitions())
    print(rdd.glom().collect())