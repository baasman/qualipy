from functools import wraps
from typing import Any, Dict, List, Callable, Optional, Union

import pandas as pd

from qualipy._sql import SQLite
from qualipy.util import copy_function_spec, import_function_by_name

from qualipy.backends.pandas_backend.pandas_types import (
    FloatType as pFloatType,
    ObjectType as pObjectType,
    IntType as pIntType,
    BoolType as pBoolType,
)
from qualipy.config import DEFAULT_CAT_FUNCTIONS, DEFAULT_NUM_FUNCTIONS


SQL = {"sqlite": SQLite}


# TODO: make dataclass for column dict structure


def function(
    allowed_arguments: Optional[List[str]] = None,
    return_format: type = float,
    arguments: Dict[str, Any] = None,
    anomaly: bool = False,
    other_column: Union[Optional[List[str]], Optional[str]] = None,
    fail: bool = False,
) -> Callable:
    def inner_fun(method: Callable):
        method.anomaly = anomaly
        method.allowed_arguments = (
            [] if allowed_arguments is None else allowed_arguments
        )
        method.arguments = {} if arguments is None else arguments
        method.has_decorator = True
        method.return_format = return_format
        method.other_column = other_column
        method.fail = fail

        @wraps(method)
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)

        return wrapper

    return inner_fun


class Column(object):

    column_name = None
    column_type = None
    force_type = False
    null = True
    force_null = False
    unique = False
    is_category = False
    functions = []

    def _as_dict(self, name: str, read_functions: bool = True) -> Dict[str, Any]:
        dict_ = {
            "name": name,
            "type": self.column_type,
            "force_type": self.force_type,
            "null": self.null,
            "force_null": self.force_null,
            "unique": self.unique,
            "is_category": self.is_category,
            "functions": self._get_functions() if read_functions else self.functions,
        }
        return dict_

    def _from_dict(self, args: Dict):
        for key, val in args.items():
            setattr(self, key, val)

    def _get_functions(self) -> Dict[str, Callable]:
        methods = {}
        for fun in dir(self):
            function = getattr(self, fun, None)
            if getattr(function, "has_decorator", False):
                methods[fun] = function
        given_methods = getattr(self, "functions", None)
        if given_methods:
            for func in given_methods:
                copied_function = copy_function_spec(func)
                methods[copied_function.__name__] = copied_function
        return methods


class Table(object):

    columns = "all"
    infer_schema = True
    table_name = None
    time_column = None
    _columns = []

    def _infer_columns(self, data):
        pass

    def _import_function(self, function_name):
        pass

    def extract_sample_row(self):
        pass


class PandasTable(Table):

    columns = "all"
    infer_schema = True
    table_name = None
    data_source = "pandas"
    time_column = None
    ignore = []

    _INFER_TYPES = {
        "float64": pFloatType,
        "int64": pIntType,
        "object": pObjectType,
        "bool": pBoolType,
    }
    _columns = []

    def _infer_columns(self, data: pd.DataFrame):
        self.columns = [
            col
            for col in data.columns
            if col != self.time_column and col not in self.ignore
        ]
        for col in self.columns:
            is_cat = True if data[col].dtype.name == "object" else False
            column = Column()
            column._from_dict(
                {
                    "name": col,
                    "column_type": self._INFER_TYPES[data[col].dtype.name](),
                    "force_type": False,
                    "null": True,
                    "force_null": False,
                    "unique": False,
                    "is_category": is_cat,
                    "functions": DEFAULT_CAT_FUNCTIONS
                    if is_cat
                    else DEFAULT_NUM_FUNCTIONS,
                }
            )
            self._columns.append(column)

    def _import_function(self, function_name):
        function = import_function_by_name(function_name, "pandas")
        function.key_function = False
        function.parameters = {}
        return function


class SQLTable(object):

    columns = "all"
    infer_schema = True
    table_name = None
    time_column = None

    _INFER_TYPES = {"float64": pFloatType, "int64": pIntType, "object": pObjectType}
    _columns = []

    def _infer_columns(self, engine) -> Dict[str, Any]:
        sql = SQL[self.db]
        row = sql.get_top_row(engine, self.columns, self.table_name)
        all_columns = {}
        for col in row.columns:
            is_cat = True if row[col].dtype.name == "object" else False
            all_columns[col] = {
                "name": col,
                "type": self._INFER_TYPES[row[col].dtype.name](),
                "force_type": False,
                "null": True,
                "force_null": False,
                "unique": False,
                "is_category": is_cat,
                "functions": DEFAULT_CAT_FUNCTIONS if is_cat else DEFAULT_NUM_FUNCTIONS,
            }
        self.columns_to_select = list(all_columns.keys())
        return all_columns
