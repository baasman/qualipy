from functools import wraps

from typing import Any, Dict, List, Callable, Optional, Union

from qualipy._sql import SQLite
from qualipy.util import copy_function_spec

# TODO: definitely need to abstract this
from qualipy.backends.pandas_backend.pandas_types import FloatType, ObjectType, IntType
from qualipy.backends.pandas_backend.functions import (
    mean,
    std,
    percentage_missing,
    value_counts,
)


SQL = {"sqlite": SQLite}

INFER_TYPES = {"float64": FloatType, "int64": IntType, "object": ObjectType}

DEFAULT_NUM_FUNCTIONS = [mean, std, percentage_missing]
DEFAULT_CAT_FUNCTIONS = [value_counts]


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

    def _as_dict(self, name: str) -> Dict[str, Any]:
        dict_ = {
            "name": name,
            "type": self.column_type,
            "force_type": self.force_type,
            "null": self.null,
            "force_null": self.force_null,
            "unique": self.unique,
            "is_category": self.is_category,
            "functions": self._get_functions(),
        }
        return dict_

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
    db = "sqlite"

    def _infer_columns(self, engine):
        sql = SQL[self.db]
        row = sql.get_top_row(engine, self.columns, self.table_name)
        all_columns = {}
        for col in row.columns:
            is_cat = True if row[col].dtype.name == "object" else False
            all_columns[col] = {
                "name": col,
                "type": INFER_TYPES[row[col].dtype.name](),
                "force_type": False,
                "null": True,
                "force_null": False,
                "unique": False,
                "is_category": is_cat,
                "functions": DEFAULT_CAT_FUNCTIONS if is_cat else DEFAULT_NUM_FUNCTIONS,
            }
