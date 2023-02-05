from pyspark import  SparkConf,SparkContext
if __name__ == '__main__' :
    conf = SparkConf().setMaster('local[*]').setAppName('hellow_wrold')
    sc = SparkContext(conf = conf)
    file_rdd = sc.textFile('word.txt',100)
    print(file_rdd.getNumPartitions())
    word_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    one_rdd = word_rdd.map(lambda x: (x,1))
    res_rdd = one_rdd.reduceByKey(lambda a,b:a+b)   # 3, 3*2+1 = 7
    print(res_rdd.collect())