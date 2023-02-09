from pyspark import SparkContext,SparkConf
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("sss2")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize(range(1,20))
    rdd2 = sc.parallelize([('a',1),('b',2),('a',1)])
    rdd2.groupByKey().mapValues(list)
    print(rdd2.collect())
    print(rdd2.foreach(lambda x: x[1] * 10))
    print(rdd2.groupBy(lambda x:x[0]).mapValues(list).collect())
    print(rdd2.reduceByKey(lambda a,b:a+b).collect())
    print(rdd.reduce(lambda a, b: a + b))