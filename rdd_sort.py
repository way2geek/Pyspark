from pyspark import SparkContext, SparkConf



if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([('a', 123), ('b', 321), ('c', 999), ('d', 1222), ('A', 23), ])
    rdd2 = sc.parallelize([('a', 321), ('b', 3212), ('c', 2999)])


    def process(input):
        return str(input).lower()


    print(rdd1.sortByKey(ascending=True, numPartitions=1,keyfunc=lambda x:process(x)).collect())
    print(rdd1.sortByKey(ascending=True, numPartitions=1).collect())
