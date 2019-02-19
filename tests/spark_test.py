import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, StringType
import pyspark.sql.functions as F

def mean(df, column):
    return df.agg(F.mean(F.col(column)).alias('mean')).collect()[0]['mean']


def nunique(df, column):
    return len(df.agg(F.collect_set(column).alias(column)).first()[column])



if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("test") \
        .getOrCreate()

    schema = StructType(
        [
            StructField('sepal_length', DoubleType()),
            StructField('sepal_width', DoubleType()),
            StructField('petal_length', DoubleType()),
            StructField('petal_width', DoubleType()),
            StructField('variety', StringType()),
        ]
    )
    df = spark.read.csv('/Users/baasman/PycharmProjects/qualipy/tests/iris.csv',
                        header=True, schema=schema)


    avg = nunique(df, 'variety')
    print(avg)

    # print(df)
    # mean = df.agg({'sepal_width': 'avg'})
    # print(mean.collect()[0]['avg(sepal_width)'])
    # mean = df.agg(F.col(F.avg('sepal_width')))
    # print(mean)