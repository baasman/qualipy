import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, StringType

from qualipy import DataSet
import random
import pandas as pd


iris = {
    'data_name': 'iris-spark',
    'columns': {
        'petal_length': {
            'type': 'float',
            'metrics': [
                'mean',
                'std',
            ]
        },
        'variety': {
            'type': 'string',
            'metrics': ['nunique']
        }
    },
}
if __name__ == '__main__':

    ts = pd.date_range(start='1/1/2018', end='1/30/2018')

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
    for idx, time in enumerate(ts):
        print(time)
        iris_data = spark.read.csv('/Users/baasman/PycharmProjects/qualipy/tests/iris.csv',
                            header=True, schema=schema)
        ran = random.randint(0, 5)
        iris_data = iris_data.withColumn('petal_length', iris_data.petal_length + ran)
        if idx == 0:
            ds = DataSet(iris, reset=True, time_of_run=time, backend='spark')
        else:
            ds = DataSet(iris, reset=False, time_of_run=time, backend='spark')
        ds.set_dataset(iris_data)
        ds.run()
