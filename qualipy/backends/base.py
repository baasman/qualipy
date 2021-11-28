import abc
import datetime
import pickle
import uuid
import json
from typing import Tuple, Dict, Union, Callable, List, Optional, Any
import copy

import pandas as pd
from numpy import NaN

from qualipy.exceptions import InvalidReturnValue


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
        self.config = config

    @abc.abstractmethod
    def set_schema(self, data, columns):
        return

    def generate_description(
        self,
        function: Callable,
        data,
        column: str,
        date: datetime.datetime,
        function_name: str,
        viz_type: str = "numerical",
        return_format: str = "float",
        kwargs: Dict[str, Any] = None,
        overwrite_kwargs: Dict[str, Any] = None,
    ):
        kwargs = {} if kwargs is None else kwargs
        original_kwargs = copy.copy(kwargs)
        if overwrite_kwargs is not None:
            for arg in function.allowed_arguments:
                if arg in overwrite_kwargs:
                    kwargs[arg] = overwrite_kwargs[arg]
        value = function(data, column, **kwargs)
        return {
            "value": value,
            "metric": function_name,
            "arguments": _create_arg_string(original_kwargs),
            "date": date,
            "column_name": column,
            "return_format": return_format,
            "type": viz_type,
        }

    def set_return_value_type(self, value: type, return_format: type):
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
            if not isinstance(value, list):
                raise InvalidReturnValue("Improperly formatted custom return type")
        else:
            raise InvalidReturnValue(
                "Unsupported type: '{}'".format(str(return_format))
            )
        return value

    @abc.abstractmethod
    def get_dtype(self, data, column):
        return

    @abc.abstractmethod
    def check_type(self, data, column, desired_type, force=False):
        return

    @abc.abstractmethod
    def generate_column_general_info(self, specs, data, time_of_run):
        return

    @abc.abstractmethod
    def generate_data(self, *args, **kwargs):
        return

    def write(self, conn, measures, project, batch_name, schema=None):
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
