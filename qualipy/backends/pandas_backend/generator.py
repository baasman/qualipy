from qualipy.exceptions import InvalidReturnValue
from qualipy.util import get_column
from qualipy.backends.base import BackendBase
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
    def get_other_columns(
        other_column: Optional[Union[List[str], str]],
        arguments: Dict[str, Any],
        data: pd.DataFrame,
    ):

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
        data: pd.DataFrame, columns: Dict[str, Column]
    ) -> Dict[str, Dict[str, Union[bool, str]]]:
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
        function: Callable,
        data: pd.DataFrame,
        column: str,
        date: datetime.datetime,
        function_name: str,
        standard_viz: bool,
        is_static: bool = True,
        other_columns: Optional[Dict[str, Any]] = None,
        viz_type: str = "numerical",
        return_format: str = "float",
        key_function: bool = False,
        kwargs: Dict[str, Any] = None,
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
            "return_format": return_format,
            "is_static": is_static,
            "key_function": key_function,
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

    @staticmethod
    def generate_column_general_info(specs, data, time_of_run):
        col_name = specs["name"]
        if specs["unique"]:
            unique = BackendPandas.generate_description(
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
            value_props = BackendPandas.generate_description(
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
        perc_missing = BackendPandas.generate_description(
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

    @staticmethod
    def write(measures, project, batch_name):
        data = pd.DataFrame(measures)
        data["insert_time"] = datetime.datetime.now().replace(tzinfo=None)
        value_ids = [uuid.uuid4() for _ in range(data.shape[0])]
        data["batch_name"] = batch_name
        data["valueID"] = value_ids
        data.valueID = data.valueID.astype(str)

        value_data = data[
            data.type.isin(["numerical", "boolean", "data-characteristic"])
        ][["valueID", "value"]]
        value_data.value = value_data.value.astype(str)

        value_data_custom = data[data.type.isin(["categorical"])][["valueID", "value"]]
        value_data_custom.value = value_data_custom.value.apply(
            lambda v: pickle.dumps(v)
        )

        data = data.drop("value", axis=1)

        # data.value = data.value.apply(lambda v: pickle.dumps(v))
        with project.engine.begin() as conn:
            data.to_sql(project.project_name, con=conn, if_exists="append", index=False)
            value_data.to_sql(
                project.value_table, con=conn, if_exists="append", index=False
            )
            value_data_custom.to_sql(
                project.value_custom_table, con=conn, if_exists="append", index=False
            )
