from pyspark import  SparkConf,SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName('test1').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    rdd = sc.wholeTextFiles("./data")
    rdd1 = rdd.map(lambda x: x[1])
    print(rdd1.collect())
    rdd2 = rdd1.map(lambda line:line.split(" "))
    print(rdd2.collect())