from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    session = SparkSession.builder.appName('df').master('local[*]').getOrCreate()
    sc = session.sparkContext
    rdd = sc.textFile('../data/stu_score.txt')
    rdd1 = rdd.map(lambda x:x.split(',')).\
        map(lambda x:[int(x[0]),x[1],int(x[2])])
    schema = StructType().add('id',IntegerType(),nullable=False).\
        add('subject',StringType(),nullable=False).\
        add('score',IntegerType(),nullable=False)
    df = session.createDataFrame(rdd1,schema=schema)
    df.printSchema()
    df.show(10,False)