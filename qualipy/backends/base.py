import abc
import datetime
import pickle
import uuid
from typing import Tuple, Dict, Union, Callable, List, Optional, Any

import pandas as pd
from numpy import NaN


def _create_arg_string(keyword_arguments: Dict[str, Any]) -> str:
    if keyword_arguments:
        return str(keyword_arguments)
    return NaN


class BaseType(object):
    def __repr__(self):
        class_name = str(self.__class__).split(".")[-1]
        class_name = class_name.replace("'>", "")
        return class_name


class BackendBase(abc.ABC):
    def __init__(self, config):
        pass

    @staticmethod
    @abc.abstractmethod
    def set_schema(data, columns):
        return

    @staticmethod
    def generate_description(
        function: Callable,
        data,
        column: str,
        date: datetime.datetime,
        function_name: str,
        standard_viz: bool,
        is_static: bool = True,
        viz_type: str = "numerical",
        return_format: str = "float",
        key_function: bool = False,
        kwargs: Dict[str, Any] = None,
    ):
        kwargs = {} if kwargs is None else kwargs
        value = function(data, column, **kwargs)
        return {
            "value": value,
            "metric": function_name,
            "arguments": _create_arg_string(kwargs),
            "date": date,
            "column_name": column,
            "standard_viz": standard_viz,
            "return_format": return_format,
            "is_static": is_static,
            "key_function": key_function,
            "type": viz_type,
        }

    @staticmethod
    @abc.abstractmethod
    def set_return_value_type(value, return_format):
        return

    @staticmethod
    @abc.abstractmethod
    def get_dtype(data, column):
        return

    @staticmethod
    @abc.abstractmethod
    def check_type(data, column, desired_type, force=False):
        return

    @staticmethod
    @abc.abstractmethod
    def generate_column_general_info(specs, data, time_of_run):
        return

    @staticmethod
    def write(conn, measures, project, batch_name, schema=None):
        data = pd.DataFrame(measures)
        data["insert_time"] = datetime.datetime.now().replace(tzinfo=None)
        value_ids = [uuid.uuid4() for _ in range(data.shape[0])]
        data["batch_name"] = batch_name
        data["value_id"] = value_ids
        data.value_id = data.value_id.astype(str)

        value_data = data[
            data.type.isin(["numerical", "boolean", "data-characteristic"])
        ][["value_id", "value"]]
        value_data.value = value_data.value.astype(str)

        value_data_custom = data[data.type.isin(["categorical"])][["value_id", "value"]]
        value_data_custom.value = value_data_custom.value.apply(
            lambda v: pickle.dumps(v)
        )

        data = data.drop("value", axis=1)

        data.to_sql(
            project.project_name,
            con=conn,
            if_exists="append",
            index=False,
            schema=schema,
        )
        value_data.to_sql(
            project.value_table,
            con=conn,
            if_exists="append",
            index=False,
            schema=schema,
        )
        value_data_custom.to_sql(
            project.value_custom_table,
            con=conn,
            if_exists="append",
            index=False,
            schema=schema,
        )
