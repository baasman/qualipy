from qualipy.metrics import PANDAS_METRIC_MAP


dtypes = {
    'float': float,
    'int': int,
    'string': str
}

class GeneratorPandas():

    @staticmethod
    def set_type(data, column, type):
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
            fun = PANDAS_METRIC_MAP[measure]
        return {
            'value': fun(data, column, **kwargs),
            '_metric': metric_name
        }

