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
    def set_schema(
        self, data: pd.DataFrame, columns: Dict[str, Column], current_name: str
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

    def get_shape(self, data):
        rows, cols = data.shape
        return rows, cols

    def get_dtype(self, data, column):
        return data[column].dtype

    def check_type(self, data, column, desired_type, force=False):
        is_equal = desired_type.check_approximate_type(data[column].dtype)
        if is_equal:
            return
        if force and not is_equal:
            raise InvalidType(
                "Incorrect type for column {}. Expected {}, "
                "got {}".format(column, desired_type, data[column].dtype)
            )

    def generate_column_general_info(self, specs, data, time_of_run, run_name):
        col_name = specs["name"]
        if specs["unique"]:
            unique = self.generate_description(
                function=is_unique,
                data=data,
                column=col_name,
                function_name="is_unique",
                date=time_of_run,
                viz_type="data-characteristic",
                kwargs={},
            )
            unique["run_name"] = run_name
        else:
            unique = None
        if specs["is_category"]:
            value_props = self.generate_description(
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
            if value_props is not None:
                value_props["run_name"] = run_name
        else:
            value_props = None
        perc_missing = self.generate_description(
            function=percentage_missing,
            data=data,
            column=col_name,
            function_name="perc_missing",
            date=time_of_run,
            viz_type="data-characteristic",
            kwargs={},
        )
        perc_missing["run_name"] = run_name
        return unique, perc_missing, value_props

    def get_chunks(self, data, time_freq, time_column):
        if data.shape[0] == 0:
            raise Exception("Unable to chunk empty dataframe")
        groups = [
            {"batch_name": d[0], "chunk": d[1]}
            for d in list(data.groupby(pd.Grouper(key=time_column, freq=time_freq)))
        ]
        return groups

    def overwrite_type(self, data, col, type):
        if isinstance(type, FloatType) or isinstance(type, IntType):
            data[col] = pd.to_numeric(data[col], errors="coerce")
        else:
            data[col] = data[col].astype(type.str_name)
        return data

    def generate_data(self, data, config):
        return

    def profile_batch(
        self, data, batch_name, run_name, columns, config_dir, project_name
    ):
        profiler = PandasBatchProfiler(
            data, batch_name, run_name, columns, config_dir, project_name
        )
        profiler.profile()

    def return_split_subset(self, data, split_var, split_value):
        data = data[data[split_var] == split_value].copy()
        return data

    def return_data_copy(self, data):
        return data.copy()
