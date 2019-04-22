from qualipy.metrics import PANDAS_METRIC_MAP
from qualipy.util import get_column

import numpy as np


dtypes = {
    'float': float,
    'int': int,
    'string': str
}

class GeneratorPandas():

    @staticmethod
    def set_type(data, column, type):
        """
        type should be a valid numpy/pandas type
        """
        if column == 'index':
            data.index = data.index.astype(type)
        else:
            data[column] = data[column].astype(type)
        return data

    @staticmethod
    def generate_description(data, column, measure, custom_funcs=None, kwargs=None):
        arguments = str(kwargs) if kwargs else np.NaN
        metric_name = measure
        if measure in custom_funcs:
            fun = custom_funcs[measure]
        else:
            fun = PANDAS_METRIC_MAP[measure]
        return {
            'value': fun(data, column, **kwargs),
            '_metric': metric_name,
            '_arguments': arguments
        }

