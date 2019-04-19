from qualipy.metrics import PANDAS_METRIC_MAP
from qualipy.util import get_column


dtypes = {
    'float': float,
    'int': int,
    'string': str
}

class GeneratorPandas():

    @staticmethod
    def set_type(data, column, type):
        if column == 'index':
            data.index = data.index.astype(dtypes[type])
        else:
            data[column] = data[column].astype(dtypes[type])
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
            try:
                fun = PANDAS_METRIC_MAP[measure]
            except:
                print(measure)
        return {
            'value': fun(data, column, **kwargs),
            '_metric': metric_name
        }

