from qualipy.exceptions import InvalidReturnValue
from qualipy.util import get_column
from qualipy.backends.base import BackendBase

from qualipy.backends.pandas_backend.dataset import PandasData
from qualipy.backends.pandas_backend.pandas_types import FloatType, IntType
from qualipy.backends.pandas_backend.batch_profiler import PandasBatchProfiler
from qualipy.exceptions import InvalidType
from qualipy.backends.pandas_backend.functions import (
    is_unique,
    percentage_missing,
    value_counts,
)

import warnings
import datetime
from typing import Optional, Union, List, Dict, Any, Callable
import uuid
import pickle

from numpy import NaN
import pandas as pd


Column = Dict[str, Union[str, bool, Dict[str, Callable]]]


class BackendPandas(BackendBase):
    def __init__(self, config):
        pass

    @staticmethod
    def set_return_value_type(value: type, return_format: type):
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
    def set_schema(
        data: pd.DataFrame, columns: Dict[str, Column], current_name: str
    ) -> Dict[str, Dict[str, Union[bool, str]]]:
        # TODO: figure out what to do if column name is a list
        schema = {
            f"{info['name']}_{current_name}": {
                "nullable": info["null"],
                "unique": info["unique"],
                "dtype": str(get_column(data, info["name"]).dtype),
                "column_name": info["name"],
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
    def check_type(data, column, desired_type, force=False):
        is_equal = desired_type.check_approximate_type(data[column].dtype)
        if is_equal:
            return
        if force and not is_equal:
            raise InvalidType(
                "Incorrect type for column {}. Expected {}, "
                "got {}".format(column, desired_type, data[column].dtype)
            )

    @staticmethod
    def generate_column_general_info(specs, data, time_of_run):
        col_name = specs["name"]
        if specs["unique"]:
            unique = BackendPandas.generate_description(
                function=is_unique,
                data=data,
                column=col_name,
                function_name="is_unique",
                date=time_of_run,
                viz_type="data-characteristic",
                kwargs={},
            )
        else:
            unique = None
        if specs["is_category"]:
            value_props = BackendPandas.generate_description(
                function=value_counts,
                data=data,
                column=col_name,
                function_name="value_counts",
                date=time_of_run,
                viz_type="categorical",
                kwargs={},
                return_format="dict",
            )
            value_props = None if str(value_props["value"]) == "nan" else value_props
        else:
            value_props = None
        perc_missing = BackendPandas.generate_description(
            function=percentage_missing,
            data=data,
            column=col_name,
            function_name="perc_missing",
            date=time_of_run,
            viz_type="data-characteristic",
            kwargs={},
        )
        return unique, perc_missing, value_props

    @staticmethod
    def get_chunks(data, time_freq, time_column):
        if data.shape[0] == 0:
            raise Exception("Unable to chunk empty dataframe")
        groups = [
            {"batch_name": d[0], "chunk": d[1]}
            for d in list(data.groupby(pd.Grouper(key=time_column, freq=time_freq)))
        ]
        return groups

    @staticmethod
    def overwrite_type(data, col, type):
        if isinstance(type, FloatType) or isinstance(type, IntType):
            data[col] = pd.to_numeric(data[col], errors="coerce")
        else:
            data[col] = data[col].astype(type.str_name)
        return data

    @staticmethod
    def generate_data(data, config):
        return

    @staticmethod
    def profile_batch(data, batch_name, run_name, columns, config_dir, project_name):
        profiler = PandasBatchProfiler(
            data, batch_name, run_name, columns, config_dir, project_name
        )
        profiler.profile()
