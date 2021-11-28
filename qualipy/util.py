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


def set_metric_id(data):
    data["metric_id"] = (
        data.column_name
        + "_"
        + data.metric.astype(str)
        + "_"
        # + np.where(
        #     data.arguments.isnull(),
        #     "",
        #     data.arguments.astype(str).str.replace(" ", "").str.replace("'", ""),
        # )
    )
    return data


def set_value_type(data: pd.DataFrame) -> pd.DataFrame:
    type = data.return_format.values[0]
    data_type = data.type.values[0]
    if type == "bool":
        data.value = data.value.map(
            {"True": True, "False": False, "true": True, "false": False}
        )
    elif type == "dict" and data_type != "numerical":
        data.value = data.value.apply(lambda v: json.loads(v))
    elif type == "dict" and data_type == "numerical":
        data.value = data.value.astype(float)
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
        copied_function.valid_min_range = function.get("valid_min")
        copied_function.valid_max_range = function.get("valid_max")
    else:
        copied_function = copy_func(function)
        copied_function.arguments = {}
        copied_function.key_function = False
    return copied_function


def import_function_by_name(name: str, backend: str) -> Callable:
    module = importlib.import_module(f"qualipy.backends.{backend}_backend.functions")
    return getattr(module, name)


def get_latest_insert_only(data, floor_datetime=False):
    group_name = "batch_name"
    if floor_datetime:
        data["floored_datetime"] = data.date.dt.floor("T")
        group_name = "floored_datetime"
    data = (
        data.groupby(group_name, as_index=False)
        .apply(lambda g: g[g.insert_time == g.insert_time.max()])
        .reset_index(drop=True)
    )
    if "floored_datetime" in data.columns:
        data = data.drop("floored_datetime", axis=1)
    return data


def get_project_data(
    project, timezone=None, latest_insert_only=False, floor_datetime=False
):
    timezone = "UTC" if timezone is None else timezone
    data = project.get_project_table()
    try:
        data.date = pd.to_datetime(data.date).dt.tz_convert(timezone)
    except TypeError:
        data.date = pd.to_datetime(data.date)
    try:
        data.insert_time = pd.to_datetime(data.insert_time).dt.tz_convert(timezone)
    except TypeError:
        data.insert_time = pd.to_datetime(data.insert_time)
    except ValueError:
        data.insert_time = pd.to_datetime(data.insert_time, utc=True)
    data.value = data.value.fillna(np.NaN)
    data["original_column_name"] = data["column_name"]
    data["column_name"] = data["column_name"] + "_" + data["run_name"]
    data.batch_name = np.where(
        data.batch_name == "from_chunked", data.date.astype(str), data.batch_name
    )
    data = data.sort_values("batch_name")
    data = set_metric_id(data)
    if latest_insert_only:
        data = get_latest_insert_only(data, floor_datetime=floor_datetime)
    return data


def get_anomaly_data(project, timezone=None):
    timezone = "UTC" if timezone is None else timezone
    data = project.get_anomaly_table()
    try:
        data.date = pd.to_datetime(data.date).dt.tz_convert(timezone)
    except TypeError:
        data.date = pd.to_datetime(data.date)
    except ValueError:
        data.date = pd.to_datetime(data.date, utc=True)
    try:
        data.insert_time = pd.to_datetime(data.insert_time).dt.tz_convert(timezone)
    except TypeError:
        data.date = pd.to_datetime(data.insert_time)
    except ValueError:
        data.insert_time = pd.to_datetime(data.insert_time, utc=True)
    data.batch_name = np.where(
        data.batch_name == "from_chunked", data.date.astype(str), data.batch_name
    )
    data = data.sort_values("batch_name")
    data = set_metric_id(data)
    return data


def set_title_name(data):
    data = data.iloc[0]
    run_name = data["run_name"].capitalize()
    var_name = data["column_name"].replace(f"_{data['run_name']}", "")
    metric_name = data["metric"]
    arguments = "" if data["arguments"] is None else f"_{data['arguments']}"
    title = f"{run_name}: {var_name} - {metric_name}{arguments}"
    return title


def setup_logging():
    """Initialize logging settings."""
    # Setup logging
    from logging import basicConfig
    from rich.console import Console
    from rich.logging import RichHandler

    console = Console(width=160)
    basicConfig(
        level="INFO",
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console)],
    )