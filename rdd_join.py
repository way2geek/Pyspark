from pyspark import  SparkContext,SparkConf
if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([('a',123),('b',321),('c',999),('d',1222)])
    rdd2 = sc.parallelize([('a', 321), ('b', 3212), ('c', 2999)])
    print(rdd1.leftOuterJoin(rdd2).collect())
    print(rdd1.join(rdd2).collect())
    print(rdd1.intersection(rdd2).collect())