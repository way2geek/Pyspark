from pyspark import SparkContext,SparkConf
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("ss")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([('hadoop',1),('spark',10),('spark',10),('flink',100)])
    rdd = rdd.groupBy(lambda x:x[0])
    print(rdd.map(lambda x:(x[0],list(x[1]))).collect())
