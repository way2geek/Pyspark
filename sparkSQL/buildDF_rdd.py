from pyspark.sql import SparkSession

if __name__ == '__main__':
    session = SparkSession.builder.appName('df').master('local[*]').getOrCreate()
    sc = session.sparkContext
    rdd = sc.textFile('../data/stu_score.txt')
    rdd1 = rdd.map(lambda x:x.split(',')).\
        map(lambda x:[int(x[0]),x[1],int(x[2])])
    df =session.createDataFrame(rdd1,schema=['id','subject','score'])
    df.show(5,False)  #flase不截断，全部显示
    df.printSchema()