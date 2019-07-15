from qualipy.exceptions import InvalidReturnValue
from qualipy.util import get_column
from qualipy.backends.base import BackendBase
from qualipy.exceptions import InvalidType

import warnings
from typing import Optional, Union, List, Dict, Any

from numpy import NaN
import pandas as pd


def _create_arg_string(
    keyword_arguments: Dict[str, Any], other_columns: Optional[List[str]] = None
) -> str:
    if keyword_arguments:
        if other_columns is not None:
            col_arguments = {
                "col_{}".format(idx): k for idx, k in enumerate(other_columns)
            }
            for col in other_columns:
                keyword_arguments.pop(col)
            keyword_arguments = {**keyword_arguments, **col_arguments}
        return str(keyword_arguments)
    return NaN


def _set_columns(other_column, other_columns, arguments, data):
    if other_column in arguments:
        other_columns[other_column] = data[arguments[other_column]]
    else:
        other_columns[other_column] = data[other_column]
    return other_columns


class BackendPandas(BackendBase):
    @staticmethod
    def get_other_columns(other_column, arguments, data):

        if other_column is not None:
            other_columns = {}
            if not isinstance(other_column, list):
                other_column = [other_column]
            for col in other_column:
                other_columns = _set_columns(col, other_columns, arguments, data)
        else:
            other_columns = None
        return other_columns

    @staticmethod
    def set_return_value_type(value, return_format):
        if return_format in [int, float, str, dict, bool]:
            try:
                value = return_format(value)
            except TypeError as e:
                raise InvalidReturnValue(
                    "Invalid return value: {}, was expecting"
                    " '{}'".format(e, str(return_format))
                )
        elif return_format == "custom":
            pass
        else:
            raise InvalidReturnValue(
                "Unsupported type: '{}'".format(str(return_format))
            )
        return value

    @staticmethod
    def set_schema(data, columns):
        schema = {
            col: {
                "nullable": info["null"],
                "unique": info["unique"],
                "dtype": str(get_column(data, col).dtype),
            }
            for col, info in columns.items()
        }
        return schema

    @staticmethod
    def get_shape(data):
        rows, cols = data.shape
        return rows, cols

    @staticmethod
    def get_dtype(data, column):
        return data[column].dtype

    @staticmethod
    def generate_description(
        function,
        data,
        column,
        date,
        function_name,
        standard_viz,
        is_static=True,
        other_columns=None,
        viz_type="numerical",
        kwargs=None,
    ):
        kwargs = {} if kwargs is None else kwargs
        if other_columns is not None:
            kwargs = {**kwargs, **other_columns}
        value = function(data, column, **kwargs)
        return {
            "value": value,
            "metric": function_name,
            "arguments": _create_arg_string(kwargs, other_columns),
            "date": date,
            "column_name": column,
            "standard_viz": standard_viz,
            "is_static": is_static,
            "type": viz_type,
        }

    @staticmethod
    def check_type(data, column, desired_type, force=False):
        is_equal = desired_type.check_approximate_type(data[column].dtype)
        if is_equal:
            return
        elif force and not is_equal:
            raise InvalidType(
                "Incorrect type for column {}. Expected {}, "
                "got {}".format(column, desired_type, data[column].dtype)
            )
        elif not force and not is_equal:
            # TODO: make warning message more useful
            warnings.warn("Type is not equal")
