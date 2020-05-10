from qualipy.backends.base import BackendBase
from qualipy.exceptions import InvalidType, InvalidReturnValue
from qualipy.column import function
from qualipy.backends.spark_backend.functions import (
    is_unique,
    percentage_missing,
    value_counts,
)

import pyspark
import pandas as pd
from numpy import NaN

from typing import Tuple, Dict, Union, Callable, List, Optional, Any
import datetime
import json
import os
import uuid
import pickle
import warnings


DataFrame = pyspark.sql.dataframe.DataFrame
Column = Dict[str, Union[str, bool, Dict[str, Callable]]]


class BackendSpark(BackendBase):
    def __init__(self, config):
        with open(os.path.join(config, "config.json"), "rb") as f:
            loaded_config = json.load(f)
        app_name = loaded_config.get("APP_NAME", "qualipy")
        self.spark = pyspark.sql.SparkSession.builder.appName(app_name).getOrCreate()

    @staticmethod
    def get_shape(data: DataFrame) -> Tuple[int, int]:
        rows = data.count()
        columns = len(data.columns)
        return rows, columns

    @staticmethod
    def get_dtype(data: DataFrame, column: str) -> str:
        return data.schema[column].dataType.simpleString()

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

    @staticmethod
    def generate_column_general_info(specs, data, time_of_run):
        col_name = specs["name"]
        if specs["unique"]:
            unique = BackendSpark.generate_description(
                function=is_unique,
                data=data,
                column=col_name,
                standard_viz=NaN,
                function_name="is_unique",
                date=time_of_run,
                is_static=True,
                viz_type="data-characteristic",
                kwargs={},
            )
        else:
            unique = None
        if specs["is_category"]:
            value_props = BackendSpark.generate_description(
                function=value_counts,
                data=data,
                column=col_name,
                function_name="value_counts",
                standard_viz=True,
                date=time_of_run,
                is_static=True,
                viz_type="categorical",
                kwargs={},
                return_format="dict",
            )
        else:
            value_props = None
        perc_missing = BackendSpark.generate_description(
            function=percentage_missing,
            data=data,
            column=col_name,
            standard_viz=NaN,
            function_name="perc_missing",
            date=time_of_run,
            is_static=True,
            viz_type="data-characteristic",
            kwargs={},
        )
        return unique, perc_missing, value_props