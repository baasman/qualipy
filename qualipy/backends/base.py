import abc
import datetime
import pickle
import uuid
import json
from typing import Tuple, Dict, Union, Callable, List, Optional, Any

import pandas as pd
from numpy import NaN


def _create_arg_string(keyword_arguments: Dict[str, Any]) -> str:
    if keyword_arguments:
        return str(keyword_arguments)
    return NaN


def convert_value_to_varchar(value):
    if isinstance(value, dict):
        return json.dumps(value)
    return value


class BaseType(object):
    def __init__(self):
        pass

    def __repr__(self):
        class_name = str(self.__class__).split(".")[-1]
        class_name = class_name.replace("'>", "")
        return class_name


class BaseData(object):
    def __init__(self, data, config=None, stratify=None):
        self.data = data
        self.stratify = stratify

    def get_data(self, backend_used=None):
        return self.data

    def set_fallback_data(self):
        pass


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
        viz_type: str = "numerical",
        return_format: str = "float",
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
            "return_format": return_format,
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
    @abc.abstractmethod
    def generate_data(*args, **kwargs):
        return

    @staticmethod
    def write(conn, measures, project, batch_name, schema=None):
        data = pd.DataFrame(measures)
        data["insert_time"] = datetime.datetime.now().replace(tzinfo=None)
        data["batch_name"] = batch_name

        data.value = data.value.apply(convert_value_to_varchar)

        data.to_sql(
            project.project_name,
            con=conn,
            if_exists="append",
            index=False,
            schema=schema,
        )
