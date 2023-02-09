from pyspark import  SparkContext,SparkConf
if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([('a', 123), ('b', 321), ('a',1233),('c', 999), ('d', 1222)])
    print(rdd1.groupByKey().map(lambda x:(x[0],list(x[1]))).collect())
