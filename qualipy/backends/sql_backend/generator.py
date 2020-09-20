from qualipy.exceptions import InvalidReturnValue
from qualipy.util import get_column
from qualipy.backends.base import BackendBase
from qualipy.exceptions import InvalidType
from qualipy.backends.sql_backend.dataset import SQLData
from qualipy.backends.sql_backend.functions import (
    value_counts,
    percentage_missing,
    is_unique,
)

import warnings
import datetime
from typing import Optional, Union, List, Dict, Any, Callable
import uuid
import pickle

from numpy import NaN
import pandas as pd
import sqlalchemy as sa


Column = Dict[str, Union[str, bool, Dict[str, Callable]]]


class BackendSQL(BackendBase):
    def __init__(self, config):
        pass

    @staticmethod
    def set_return_value_type(value: type, return_format: type):
        if str(value) == "nan":
            return value
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
        data, columns: Dict[str, Column], current_name: str
    ) -> Dict[str, Dict[str, Union[bool, str]]]:
        # TODO: figure out what to do if column name is a list
        schema = {
            f"{info['name']}_{current_name}": {
                "nullable": info["null"],
                "unique": info["unique"],
                "dtype": BackendSQL.get_dtype(data, info["name"]),
                "column_name": info["name"],
            }
            for col, info in columns.items()
        }
        return schema

    @staticmethod
    def get_shape(data):
        if data.custom_where is None:
            count_query = sa.select([sa.func.count()]).select_from(data._table)
        else:
            count_query = (
                sa.select([sa.func.count()])
                .select_from(data._table)
                .where(sa.text(data.custom_where))
            )
        rows = int(data.engine.execute(count_query).scalar())
        columns = len(data.table_reflection)
        return rows, columns

    @staticmethod
    def get_dtype(data, column):
        try:
            col = [i for i in data.table_reflection if i["name"] == column][0]
        except IndexError:
            raise Exception(
                f"Column: {column} not found in table {data._table.fullname}"
            )
        except AttributeError:
            raise Exception(f"A pandas dataframe was pass to a SQL backend")

        return str(col["type"])

    @staticmethod
    def check_type(data, column, desired_type, force=False):
        pass

    @staticmethod
    def generate_column_general_info(specs, data, time_of_run):
        col_name = specs["name"]
        if specs["unique"]:
            unique = BackendSQL.generate_description(
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
            value_props = BackendSQL.generate_description(
                function=value_counts,
                data=data,
                column=col_name,
                function_name="value_counts",
                date=time_of_run,
                viz_type="categorical",
                kwargs={},
                return_format="dict",
            )
        else:
            value_props = None
        perc_missing = BackendSQL.generate_description(
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
        pass

    @staticmethod
    def overwrite_type(data, col, type):
        return data

    @staticmethod
    def generate_data(*args, **kwargs):
        data = SQLData(*args, **kwargs)
        return data.get_data()
