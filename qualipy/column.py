from functools import wraps
from typing import Any, Dict, List, Callable, Optional, Union, Tuple
from abc import abstractmethod, ABC

import pandas as pd
import sqlalchemy as sa

from qualipy.util import copy_function_spec, import_function_by_name

from qualipy.backends.pandas_backend.pandas_types import (
    FloatType as pFloatType,
    ObjectType as pObjectType,
    IntType as pIntType,
    BoolType as pBoolType,
    DateTimeType as pDateTimeType,
)
from qualipy.config import DEFAULT_CAT_FUNCTIONS, DEFAULT_NUM_FUNCTIONS


def function(
    allowed_arguments: Optional[List[str]] = None,
    return_format: type = float,
    arguments: Dict[str, Any] = None,
    fail: bool = False,
    valid_min_range = None,
    valid_max_range = None
) -> Callable:
    def inner_fun(method: Callable):
        method.allowed_arguments = (
            [] if allowed_arguments is None else allowed_arguments
        )
        method.arguments = {} if arguments is None else arguments
        method.has_decorator = True
        method.return_format = return_format
        method.fail = fail
        method.valid_min_range = valid_min_range
        method.valid_max_range = valid_max_range

        @wraps(method)
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)

        return wrapper

    return inner_fun


class Column(object):

    column_name = None
    column_type = None
    force_type = False
    overwrite_type = False
    null = True
    force_null = False
    unique = False
    is_category = False
    functions = []
    extra_functions = []

    def _as_dict(self, name: str, read_functions: bool = True) -> Dict[str, Any]:
        dict_ = {
            "name": self.column_name,
            "type": self.column_type,
            "force_type": self.force_type,
            "overwrite_type": self.overwrite_type,
            "null": self.null,
            "force_null": self.force_null,
            "unique": self.unique,
            "is_category": self.is_category,
            "functions": self._get_functions(column_name=name)
            if read_functions
            else self.functions,
            "extra_functions": self._get_functions("extra_functions", column_name=name),
        }
        return dict_

    def _from_dict(self, args: Dict):
        for key, val in args.items():
            setattr(self, key, val)

    def _get_functions(
        self, fun_attribute: str = "functions", column_name: str = None
    ) -> Tuple[str, Callable]:
        methods = []
        # for fun in dir(self):
        #     function = getattr(self, fun, None)
        #     if getattr(function, "has_decorator", False):
        #         methods[fun] = function
        given_methods = getattr(self, fun_attribute, None)
        if fun_attribute == "extra_functions":
            if column_name in given_methods:
                given_methods = given_methods[column_name]
            else:
                given_methods = []
        if given_methods:
            for func in given_methods:
                copied_function = copy_function_spec(func)
                methods.append((copied_function.__name__, copied_function))
        return methods


class Table(ABC):

    columns = "all"
    infer_schema = True
    table_name = None
    time_column = None
    _columns = []

    @abstractmethod
    def _generate_columns(self, data, infer):
        pass

    @abstractmethod
    def _import_function(self, function_name):
        pass

    def extract_sample_row(self):
        return

    def _get_functions(
        self, fun_attribute: str = "functions", column_name: str = None
    ) -> Dict[str, Callable]:
        methods = []
        for fun in dir(self):
            function = getattr(self, fun, None)
            if getattr(function, "has_decorator", False):
                methods[fun] = function
        given_methods = getattr(self, fun_attribute, {})
        if column_name in given_methods:
            for func in given_methods[column_name]:
                copied_function = copy_function_spec(func)
                methods.append((copied_function.__name__, copied_function))
        return methods


class PandasTable(Table):

    columns: Union[str, List[str]] = "all"
    infer_schema: bool = True
    table_name: Optional[str] = None
    data_source: str = "pandas"
    time_column: Optional[str] = None
    ignore: List[str] = []
    types: Dict = {}
    bool_as_cat: bool = False
    int_as_cat: bool = True
    extra_functions: Dict = {}

    _INFER_TYPES = {
        "float64": pFloatType,
        "int64": pIntType,
        "object": pObjectType,
        "bool": pBoolType,
        "datetime64[ns]": pDateTimeType,
    }
    _columns = []

    def from_dict(self, args: Dict):
        for key, val in args.items():
            setattr(self, key, val)

    def _generate_columns(self, extract_sample=True) -> None:
        if self.infer_schema and extract_sample:
            sample_row = self.extract_sample_row()
            self.columns = [i for i in sample_row.columns if i not in self.ignore]
        for col in self.columns:
            if col in self.types:
                col_type = self.types[col]
            else:
                try:
                    if "int" in sample_row[col].dtype.name and self.int_as_cat:
                        col_type = pObjectType()
                    else:
                        col_type = self._INFER_TYPES[sample_row[col].dtype.name]()
                except:
                    raise Exception(f"Unable to infer schema for column: {col}")
            bool_and_cat = (
                True if isinstance(col_type, pBoolType) and self.bool_as_cat else False
            )
            is_cat = True if isinstance(col_type, pObjectType) | bool_and_cat else False
            column = Column()
            column._from_dict(
                {
                    "column_name": col,
                    "column_type": col_type,
                    "force_type": False,
                    "overwrite_type": False,
                    "null": True,
                    "force_null": False,
                    "unique": False,
                    "is_category": is_cat,
                    "functions": [],
                    "extra_functions": self.extra_functions,
                }
            )
            self._columns.append(column)

    def _import_function(self, function_name):
        function = import_function_by_name(function_name, "pandas")
        function.key_function = False
        function.parameters = {}
        return function


class SQLTable(Table):

    columns: Union[str, List[str]] = "all"
    infer_schema: bool = True
    table_name: Optional[str] = None
    data_source: str = "pandas"
    time_column: Optional[str] = None
    ignore: List[str] = []
    types: Dict = {}
    bool_as_cat: bool = False
    int_as_cat: bool = True
    extra_functions: Dict = {}

    _INFER_TYPES = {}
    _columns = []

    def _generate_columns(self, extract_sample=False) -> None:
        data = self.extract_sample_row()
        column_info = {
            i["name"]: i for i in data.table_reflection if "name" in i.keys()
        }
        self.columns = [i for i in list(column_info.keys()) if i not in self.ignore]

        for col in self.columns:
            inferred_type = column_info[col]["type"]
            if isinstance(inferred_type, sa.sql.sqltypes.INTEGER) and self.int_as_cat:
                col_type = sa.sql.sqltypes.STRINGTYPE
            else:
                col_type = column_info[col]
            bool_and_cat = (
                True
                if isinstance(col_type, sa.sql.sqltypes.BOOLEANTYPE) and self.bool_as_cat
                else False
            )
            is_cat = (
                True
                if isinstance(col_type, sa.sql.sqltypes.String) | bool_and_cat
                else False
            )
            column = Column()
            column._from_dict(
                {
                    "column_name": col,
                    "column_type": col_type,
                    "force_type": False,
                    "overwrite_type": False,
                    "null": True,
                    "force_null": False,
                    "unique": False,
                    "is_category": is_cat,
                    "functions": [],
                    "extra_functions": self.extra_functions,
                }
            )
            self._columns.append(column)
        print(self)

    def _import_function(self, function_name):
        function = import_function_by_name(function_name, "sql")
        function.key_function = False
        function.parameters = {}
        return function
