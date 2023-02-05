from pyspark import SparkContext,SparkConf
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("sss2")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile('./data/order.text')
    rdd2 = rdd.flatMap(lambda x:x.split('|'))
    rdd3 = rdd2.map(lambda x: eval(x))
    rdd3 = rdd3.filter(lambda x:x['areaName']=='北京')
    rdd3 = rdd3.map(lambda x: (x['areaName'],x['category']))
    print(rdd.collect())
    print(rdd3.distinct().collect())

