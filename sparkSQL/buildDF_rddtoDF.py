from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    session = SparkSession.builder.appName('df').master('local[*]').getOrCreate()
    sc = session.sparkContext
    rdd = sc.textFile('../data/stu_score.txt')
    rdd1 = rdd.map(lambda x:x.split(',')).\
        map(lambda x:[int(x[0]),x[1],int(x[2])])
    df1 = rdd1.toDF(['id','subject','score'])
    df1.printSchema()
    df1.show()
    #传入schema对象
    schema = StructType().add('id',IntegerType(),nullable=False).\
        add('subject',StringType(),nullable=False).\
        add('score',IntegerType(),nullable=False)
    df2 = rdd1.toDF(schema)
    df2.printSchema()
    df2.show(10,False)