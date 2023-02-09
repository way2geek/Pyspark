from pyspark.sql import SparkSession

if __name__ == '__main__':
    session = SparkSession.builder.appName('df').master('local[*]').getOrCreate()
    sc = session.sparkContext
    df = session.read.csv('../data/stu_score.txt')
    df = df.toDF('id','subject','score')
    df.printSchema()
    df.limit(5).show()
    view1 = df.createTempView('score')
    session.sql("""select * from score""").show()