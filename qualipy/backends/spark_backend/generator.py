from qualipy.backends.base import BackendBase
from qualipy.backends.spark_backend.dataset import SparkData
from qualipy.exceptions import InvalidType, InvalidReturnValue
from qualipy.reflect.function import function
from qualipy.backends.spark_backend.functions import (
    is_unique,
    percentage_missing,
    value_counts,
)

import pyspark
import pandas as pd
from numpy import NaN

from typing import Tuple, Dict, Union, Callable, List, Optional, Any
import json
import os


DataFrame = pyspark.sql.dataframe.DataFrame
Column = Dict[str, Union[str, bool, Dict[str, Callable]]]


class BackendSpark(BackendBase):
    def __init__(self, config):
        self.spark = pyspark.sql.SparkSession.builder.getOrCreate()

    @staticmethod
    def get_shape(data: DataFrame) -> Tuple[int, int]:
        rows = data.table_df.count()
        columns = len(data.table_df.columns)
        return rows, columns

    @staticmethod
    def get_dtype(data: DataFrame, column: str) -> str:
        return data.table_df.schema[column.upper()].dataType.simpleString()

    @staticmethod
    def set_schema(
        data: DataFrame, columns: Dict[str, Column], current_name: str
    ) -> Dict[str, Union[bool, str]]:
        schema = {
            f"{info['name']}_{current_name}": {
                "nullable": info["null"],
                "unique": info["unique"],
                "dtype": BackendSpark.get_dtype(data, col),
                "column_name": info["name"],
            }
            for col, info in columns.items()
        }
        return schema

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
    def check_type(data, column, desired_type, force=False):
        pass

    @staticmethod
    def overwrite_type(data, col, type):
        data = data.withColumn(col, data[col].cast(type.str_name))
        return data

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
            unique.update_keys(run_name=run_name)
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
                return_format=dict,
            )
            value_props = None if str(value_props.value) == "nan" else value_props
            if value_props is not None:
                value_props.update_keys(run_name=run_name)

            distinct = self.generate_description(
                function=distinct_count,
                data=data,
                column=col_name,
                function_name="distinct_count",
                date=time_of_run,
                viz_type="data-characteristic",
                kwargs={},
                return_format=int,
            )
            distinct = None if str(distinct.value) == "nan" else distinct
            if distinct is not None:
                distinct.update_keys(run_name=run_name)
        else:
            value_props = None
            distinct = None
        perc_missing = self.generate_description(
            function=percentage_missing,
            data=data,
            column=col_name,
            function_name="perc_missing",
            date=time_of_run,
            viz_type="data-characteristic",
            kwargs={},
        )
        perc_missing.update_keys(run_name=run_name)
        return unique, perc_missing, value_props, distinct

    @staticmethod
    def generate_data(data, config):
        data = SparkData(data, config)
        return data.get_data()

    def return_data_copy(self, data):
        return data
