from pyspark import  SparkContext,SparkConf
if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([1,23,1,1,1,1,4,3425,423,5435,234,5,2345])
    rdd2 =sc.parallelize(['a','b','c'])
    rdd3 = rdd1.distinct()
    print(rdd3.collect())
    print(rdd2.union(rdd1).collect())