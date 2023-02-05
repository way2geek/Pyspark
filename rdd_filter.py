from  pyspark  import  SparkContext,SparkConf
if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,55,234,234,23,423,1,324,12,34,1324,123])
    rdd1 = rdd.filter(lambda x: x%2 == 1)
    print(rdd1.collect())
