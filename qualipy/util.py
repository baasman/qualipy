from qualipy.exceptions import InvalidColumn

import pandas as pd
import numpy as np

import os
import types
import json
from typing import Any, Dict, Callable, Optional, Union
import importlib


def get_column(data: pd.DataFrame, name: str) -> pd.Series:
    if name == "index":
        return data.index
    try:
        return data[name]
    except KeyError:
        raise InvalidColumn("Column {} is not part of the dataset".format(name))


HOME = os.path.expanduser("~")


def set_value_type(data: pd.DataFrame) -> pd.DataFrame:
    type = data.return_format.values[0]
    if type == "bool":
        data.value = data.value.map(
            {"True": True, "False": False, "true": True, "false": False}
        )
    elif type == "dict":
        data.value = data.value.apply(lambda v: json.loads(v))
    else:
        data.value = data.value.astype(type)
    return data


def copy_func(f: Callable, name: Optional[str] = None) -> Callable:
    fn = types.FunctionType(
        f.__code__, f.__globals__, name or f.__name__, f.__defaults__, f.__closure__
    )
    fn.__dict__.update(f.__dict__)
    return fn


def copy_function_spec(function: Union[Dict[str, Any], Callable]):
    if isinstance(function, dict):
        copied_function = copy_func(function["function"])
        copied_function.arguments = function.get("parameters", {})
        copied_function.key_function = function.get("key", False)
    else:
        copied_function = copy_func(function)
        copied_function.arguments = {}
        copied_function.key_function = False
    return copied_function


def import_function_by_name(name: str, backend: str) -> Callable:
    module = importlib.import_module(f"qualipy.backends.{backend}_backend.functions")
    return getattr(module, name)


def get_latest_insert_only(data):
    return (
        data.groupby("batch_name", as_index=False)
        .apply(lambda g: g[g.insert_time == g.insert_time.max()])
        .reset_index(drop=True)
    )


def get_project_data(project, timezone):
    timezone = "UTC" if timezone is None else timezone
    data = project.get_project_table()
    try:
        data.date = pd.to_datetime(data.date).dt.tz_convert(timezone)
    except TypeError:
        data.date = pd.to_datetime(data.date)
    data.insert_time = pd.to_datetime(data.insert_time)
    data.value = data.value.fillna(np.NaN)
    data["column_name"] = data["column_name"] + "_" + data["run_name"]
    data.batch_name = np.where(
        data.batch_name == "from_chunked", data.date.astype(str), data.batch_name
    )
    data = data.sort_values("batch_name")
    data["metric_id"] = (
        data.column_name
        + "_"
        + data.metric.astype(str)
        + "_"
        + np.where(data.arguments.isnull(), "", data.arguments)
    )
    return data


def get_anomaly_data(project, timezone=None):
    timezone = "UTC" if timezone is None else timezone
    data = project.get_anomaly_table()
    try:
        data.date = pd.to_datetime(data.date).dt.tz_convert(timezone)
    except TypeError:
        data.date = pd.to_datetime(data.date)
    data.insert_time = pd.to_datetime(data.insert_time)
    data.batch_name = np.where(
        data.batch_name == "from_chunked", data.date.astype(str), data.batch_name
    )
    data = data.sort_values("batch_name")
    data["metric_id"] = (
        data.column_name
        + "_"
        + data.metric.astype(str)
        + "_"
        + np.where(data.arguments.isnull(), "", data.arguments)
    )
    return data
