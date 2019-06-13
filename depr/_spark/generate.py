from pyspark.sql.types import DoubleType, IntegerType, StringType

from qualipy.metrics import SPARK_METRIC_MAP


dtypes = {
    'float': DoubleType,
    'int': IntegerType,
    'string': StringType
}

class GeneratorSpark():

    @staticmethod
    def set_type(data, column, type):
        data = data.withColumn(column, data[column].cast(dtypes[type]()))
        return data

    @staticmethod
    def generate_description(data, column, measure, custom_funcs=None, kwargs=None):
        if kwargs:
            metric_name = '{}_{}'.format(measure, str(kwargs))
        else:
            metric_name = measure
        if measure in custom_funcs:
            fun = custom_funcs[measure]
        else:
            fun = SPARK_METRIC_MAP[measure]
        return {
            'value': fun(data, column, **kwargs),
            '_metric': metric_name
        }
