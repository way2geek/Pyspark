from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd

if __name__ == '__main__':
    session = SparkSession.builder.appName('df').master('local[*]').getOrCreate()
    sc = session.sparkContext
    df = pd.DataFrame(
        {
            'id':[1,2,3],
            'subject':[11,12,34],

        }
    )
    sdf = session.createDataFrame(df)
    sdf.show()