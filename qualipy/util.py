from qualipy.exceptions import InvalidColumn

import pandas as pd

import os
import types
from typing import Any, Dict, Callable, Optional, Union


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
        data.value = data.value.map({"True": True, "False": False})
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
