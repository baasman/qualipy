import abc
import datetime
import json
from typing import Tuple, Dict, Union, Callable, List, Optional, Any
import copy

import pandas as pd
from numpy import NaN
import sqlalchemy as sa

from qualipy.exceptions import InvalidReturnValue
from qualipy.store.initial_models import Value


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


class MetricResult:

    types_repr = {
        float: "float",
        int: "int",
        bool: "bool",
        dict: "dict",
        str: "str",
        "custom": "list",
    }

    def __init__(
        self,
        value,
        metric: str,
        date: datetime.date,
        column_name: str,
        return_format: str,
        type,
        run_name: str = None,
        arguments: str = None,
        meta: dict = None,
    ) -> None:
        self.value = value
        self.metric = metric
        self.date = date
        self.column_name = column_name
        self.return_format = return_format
        self.type = type
        self.run_name = run_name
        self.arguments = arguments
        self.meta = meta

    @staticmethod
    def create_arg_string(keyword_arguments: Dict[str, Any]) -> str:
        if keyword_arguments:
            return str(keyword_arguments)
        return NaN

    def set_return_value_type(self):
        if str(self.value) == "nan":
            pass
        elif self.return_format in [int, float, str, dict, bool]:
            try:
                self.value = self.return_format(self.value)
            except TypeError as e:
                raise InvalidReturnValue(
                    "Invalid return self.value: {}, was expecting"
                    " '{}'".format(e, str(self.return_format))
                )
        elif self.return_format == "custom":
            if not isinstance(self.value, list):
                raise InvalidReturnValue("Improperly formatted custom return type")
            else:
                return
        else:
            raise InvalidReturnValue(
                "Unsupported type: '{}'".format(str(self.return_format))
            )

    def update_keys(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        return str(self.to_dict())

    def to_dict(self) -> dict:
        if self.meta is not None:
            meta = json.dumps(self.meta)
        else:
            meta = self.meta
        if isinstance(self.value, dict):
            value = json.dumps(self.value)
        else:
            value = self.value
        return {
            "value": value,
            "metric": self.metric,
            "date": self.date,
            "column_name": self.column_name,
            "return_format": self.types_repr[self.return_format],
            "type": self.type,
            "run_name": self.run_name,
            "arguments": self.arguments,
            "meta": meta,
        }


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
        return_format: str = float,
        run_name: str = None,
        kwargs: Dict[str, Any] = None,
        overwrite_kwargs: Dict[str, Any] = None,
    ) -> MetricResult:
        kwargs = {} if kwargs is None else kwargs
        original_kwargs = copy.copy(kwargs)
        if overwrite_kwargs is not None:
            for arg in function.allowed_arguments:
                if arg in overwrite_kwargs:
                    kwargs[arg] = overwrite_kwargs[arg]
        value = function(data, column, **kwargs)
        argument_str = MetricResult.create_arg_string(original_kwargs)
        metric_res = MetricResult(
            value=value,
            metric=function_name,
            arguments=argument_str,
            date=date,
            column_name=column,
            return_format=return_format,
            type=viz_type,
            run_name=run_name,
        )
        return metric_res

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
        insert_time = datetime.datetime.now().replace(tzinfo=None)
        # project_id = project.project_table.project_id
        data = []
        for measure in measures:
            value = Value(
                **{
                    **measure.to_dict(),
                    **{
                        "insert_time": insert_time,
                        "batch_name": batch_name,
                        "project": project.project_table,
                    },
                }
            )
            data.append(value)
            project.project_table.values_.append(value)

        conn.add_all(data)
        conn.add(project.project_table)
