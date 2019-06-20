from qualipy.metrics import PANDAS_METRIC_MAP
from qualipy.exceptions import InvalidReturnValue

from numpy import NaN


def _create_arg_string(keyword_arguments, other_columns=None):
    if keyword_arguments:
        if other_columns is not None:
            col_arguments = {'col_{}'.format(idx): k for idx, k in enumerate(other_columns)}
            for col in other_columns:
                keyword_arguments.pop(col)
            keyword_arguments = {**keyword_arguments, **col_arguments}
        return str(keyword_arguments)
    return NaN


class GeneratorPandas():

    @staticmethod
    def set_column_type(data, column, type):
        """
        type should be a valid numpy/pandas type
        """
        if column == 'index':
            data.index = data.index.astype(type)
        else:
            data[column] = data[column].astype(type)
        return data

    @staticmethod
    def set_return_value_type(value, return_format):
        if return_format in [int, float, str, dict, bool]:
            try:
                value = return_format(value)
            except TypeError as e:
                raise InvalidReturnValue("Invalid return value: {}, was expecting"
                                         " '{}'".format(e, str(return_format)))
        elif return_format == 'custom':
            pass
        else:
            raise InvalidReturnValue("Unsupported type: '{}'".format(str(return_format)))
        return value

    @staticmethod
    def generate_description(function, data, column,
                             date, function_name, standard_viz,
                             over_time=True, other_columns=None, kwargs=None):
        kwargs = {} if kwargs is None else kwargs
        if other_columns is not None:
            kwargs = {**kwargs, **other_columns}
        value = function(data, column, **kwargs)
        return {
            'value': value,
            '_metric': function_name,
            '_arguments': _create_arg_string(kwargs, other_columns),
            '_date': date,
            '_name': column,
            '_standard_viz': standard_viz,
            '_over_time': over_time,
        }

