from abc import abstractmethod, ABC
from typing import Dict, List, Callable, Optional, Union

import sqlalchemy as sa
import pandas as pd

from qualipy.util import copy_function_spec, import_function_by_name
from qualipy.backends.pandas_backend.pandas_types import (
    FloatType as pFloatType,
    ObjectType as pObjectType,
    IntType as pIntType,
    BoolType as pBoolType,
    DateTimeType as pDateTimeType,
)
from qualipy.reflect.column import Column, column


# class Table(ABC):

#     columns = "all"
#     infer_schema = True
#     table_name = None
#     time_column = None
#     _columns = []

#     @abstractmethod
#     def _generate_columns(self, extract_sample=False):
#         pass

#     @abstractmethod
#     def _import_function(self, function_name):
#         pass

#     def extract_sample_row(self):
#         return []

#     def _get_functions(
#         self, fun_attribute: str = "functions", column_name: str = None
#     ) -> Dict[str, Callable]:
#         methods = []
#         for fun in dir(self):
#             function = getattr(self, fun, None)
#             if getattr(function, "has_decorator", False):
#                 methods[fun] = function
#         given_methods = getattr(self, fun_attribute, {})
#         if column_name in given_methods:
#             for func in given_methods[column_name]:
#                 copied_function = copy_function_spec(func)
#                 methods.append((copied_function.__name__, copied_function))
#         return methods


# class PandasTable(Table):

#     columns: Union[str, List[str]] = "all"
#     infer_schema: bool = True
#     table_name: Optional[str] = None
#     data_source: str = "pandas"
#     time_column: Optional[str] = None
#     ignore: List[str] = []
#     types: Dict = {}
#     bool_as_cat: bool = False
#     int_as_cat: bool = True
#     extra_functions: Dict = {}

#     _INFER_TYPES = {
#         "float64": pFloatType,
#         "int64": pIntType,
#         "object": pObjectType,
#         "bool": pBoolType,
#         "datetime64[ns]": pDateTimeType,
#     }
#     _columns = []

#     def from_dict(self, args: Dict):
#         for key, val in args.items():
#             setattr(self, key, val)

#     def _generate_columns(self, extract_sample=True) -> None:
#         if self.infer_schema and extract_sample:
#             sample_row = self.extract_sample_row()
#             self.columns = [i for i in sample_row.columns if i not in self.ignore]
#         for col in self.columns:
#             if col in self.types:
#                 col_type = self.types[col]
#             else:
#                 try:
#                     if "int" in sample_row[col].dtype.name and self.int_as_cat:
#                         col_type = pObjectType()
#                     else:
#                         col_type = self._INFER_TYPES[sample_row[col].dtype.name]()
#                 except:
#                     raise Exception(f"Unable to infer schema for column: {col}")
#             bool_and_cat = (
#                 True if isinstance(col_type, pBoolType) and self.bool_as_cat else False
#             )
#             is_cat = True if isinstance(col_type, pObjectType) | bool_and_cat else False
#             column = Column()
#             column._from_dict(
#                 {
#                     "column_name": col,
#                     "column_type": col_type,
#                     "force_type": False,
#                     "overwrite_type": False,
#                     "null": True,
#                     "force_null": False,
#                     "unique": False,
#                     "is_category": is_cat,
#                     "functions": [],
#                     "extra_functions": self.extra_functions,
#                 }
#             )
#             self._columns.append(column)

#     def _import_function(self, function_name):
#         function = import_function_by_name(function_name, "pandas")
#         function.key_function = False
#         function.parameters = {}
#         return function


# class SQLTable(Table):

#     columns: Union[str, List[str]] = "all"
#     infer_schema: bool = True
#     table_name: Optional[str] = None
#     data_source: str = "pandas"
#     time_column: Optional[str] = None
#     ignore: List[str] = []
#     types: Dict = {}
#     bool_as_cat: bool = False
#     int_as_cat: bool = True
#     extra_functions: Dict = {}

#     _INFER_TYPES = {}
#     _columns = []

#     def _generate_columns(self, extract_sample=False) -> None:
#         data = self.extract_sample_row()
#         column_info = {
#             i["name"]: i for i in data.table_reflection if "name" in i.keys()
#         }
#         self.columns = [i for i in list(column_info.keys()) if i not in self.ignore]

#         for col in self.columns:
#             inferred_type = column_info[col]["type"]
#             if isinstance(inferred_type, sa.sql.sqltypes.INTEGER) and self.int_as_cat:
#                 col_type = sa.sql.sqltypes.STRINGTYPE
#             else:
#                 col_type = column_info[col]
#             bool_and_cat = (
#                 True
#                 if isinstance(col_type, sa.sql.sqltypes.BOOLEANTYPE)
#                 and self.bool_as_cat
#                 else False
#             )
#             is_cat = (
#                 True
#                 if isinstance(col_type, sa.sql.sqltypes.String) | bool_and_cat
#                 else False
#             )
#             column = Column()
#             column._from_dict(
#                 {
#                     "column_name": col,
#                     "column_type": col_type,
#                     "force_type": False,
#                     "overwrite_type": False,
#                     "null": True,
#                     "force_null": False,
#                     "unique": False,
#                     "is_category": is_cat,
#                     "functions": [],
#                     "extra_functions": self.extra_functions,
#                 }
#             )
#             self._columns.append(column)
#         print(self)

#     def _import_function(self, function_name):
#         function = import_function_by_name(function_name, "sql")
#         function.key_function = False
#         function.parameters = {}
#         return function


PANDAS_INFER_TYPES = {
    "float64": pFloatType,
    "int64": pIntType,
    "object": pObjectType,
    "bool": pBoolType,
    "datetime64[ns]": pDateTimeType,
}


class Table:
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns = columns


def pandas_table(
    columns: Union[str, List[str]] = "all",
    infer_schema: bool = True,
    table_name: Optional[str] = None,
    data_source: str = "pandas",
    time_column: Optional[str] = None,
    ignore: List[str] = [],
    types: Dict = {},
    bool_as_cat: bool = True,
    int_as_cat: Union[bool, int] = 25,
    functions: List = None,
    extra_functions: Dict = None,
    sample_dataset: pd.DataFrame = None,
):
    column_objects = []
    if extra_functions is None:
        extra_functions = {}
    if functions is None:
        functions = []
    if columns == "all":
        try:
            columns = sample_dataset.columns.tolist()
        except:
            raise Exception("Must supply sample_dataset if setting columns='all'")
    for col_name in columns:
        if col_name in types:
            col_type = types[col_name]
        elif infer_schema:
            if "int" in sample_dataset[col_name].dtype.name and (
                int_as_cat is True or isinstance(int_as_cat, int)
            ):
                if isinstance(int_as_cat, bool):
                    col_type = pObjectType()
                else:
                    n_unique = sample_dataset[col_name].nunique()
                    col_type = pIntType() if n_unique > int_as_cat else pObjectType()
            elif "bool" in sample_dataset[col_name].dtype.name and bool_as_cat is True:
                col_type = pObjectType()
            else:
                # need to add more here, should be way smarter
                col_type = PANDAS_INFER_TYPES[sample_dataset[col_name].dtype.name]()
        is_cat = True if isinstance(col_type, pObjectType) else False
        is_date = True if isinstance(col_type, pDateTimeType) else False
        column_functions = []
        for function in functions:
            if function.input_format in [int, float] and not (is_cat or is_date):
                column_functions.append(function)
            if function.input_format in [str, object] and is_cat:
                column_functions.append(function)

        column_object = column(
            column_name=col_name,
            column_type=col_type,
            force_type=False,
            overwrite_type=False,
            null=True,
            force_null=False,
            is_category=is_cat,
            is_date=is_date,
            functions=column_functions,
            extra_functions=extra_functions.get(col_name, None),
        )
        column_objects.append(column_object)

    table = Table(table_name=table_name, columns=column_objects)
    return table
