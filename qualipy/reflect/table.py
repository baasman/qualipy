from typing import Dict, List, Callable, Optional, Union

import sqlalchemy as sa
import pandas as pd

from qualipy.backends.pandas_backend.pandas_types import (
    FloatType as pFloatType,
    ObjectType as pObjectType,
    IntType as pIntType,
    BoolType as pBoolType,
    DateTimeType as pDateTimeType,
)
from qualipy.reflect.column import Column, column


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
    ignore: List[str] = None,
    types: Dict = None,
    bool_as_cat: bool = True,
    int_as_cat: Union[bool, int] = 25,
    overwrite_type: bool = False,
    functions: List = None,
    extra_functions: Dict = None,
    sample_dataset: pd.DataFrame = None,
):
    """This allows us to map an entire pandas table, without having to specify individual columns.

    Like specifying columns, this is one of the essential components of Qualipy. Using table ``allows`` us to map
    an entire dataframe at once, automatically generating the metadata for each column.

    Note - You must explicitly add it to the Project object in order for it to run.

    Args:
        columns: Can be either a list of column names you want to map, or "all", in which case
            you must supply a sample_dataset so Qualipy can infer what columns you are using.
        infer_schema: If set to True, Qualipy will infer the column types based on the pandas dtypes plus
            some other rules. Individual columns can be overwritten using the types parameter.
        table_name: Metadata for qualipy reasons. Not necessary.
        overwrite_type: This is useful if the aggregate function requires a specific datatype for it to be
            computed.
        ignore: List of columns you don't want to map. Usefull if you specified "all" for columns.
        types: Dictionary of column name to Qualipy type
        bool_as_cat: Should boolean columns be interpreted as categorical data
        int_as_cat: if set to True, all integer columns will be interpreted as categories.
            If set to an integer, only integer values with a number of unique values less than
            the set integer will be considered categories
        functions: A list of properly defined functions.
        extra_functions: If this mapping is used for multiple columns but want a function to be applied to
            only one of the columns, use this. See example for more information.
        sample_dataset: A chunk of the data must be supplied for the inference to work.

    Returns:
        A table (merely a collection of qualipy columns) object that can be added to a Project.
        See Project for more details.

    """
    column_objects = []
    if extra_functions is None:
        extra_functions = {}
    if ignore is None:
        ignore = []
    if types is None:
        types = {}
    if functions is None:
        functions = []
    if columns == "all":
        try:
            columns = [
                col for col in sample_dataset.columns.tolist() if col not in ignore
            ]
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
        is_cat = isinstance(col_type, pObjectType)
        is_date = isinstance(col_type, pDateTimeType)
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
            overwrite_type=overwrite_type,
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


def sql_table(
    table_name: str,
    engine: sa.engine.base.Engine,
    schema: str = None,
    columns: Union[str, List[str]] = "all",
    infer_schema: bool = True,
    ignore: List[str] = None,
    types: Dict = None,
    bool_as_cat: bool = True,
    int_as_cat: Union[bool, int] = 25,
    functions: List = None,
    extra_functions: Dict = None,
    sample_dataset: pd.DataFrame = None,
):
    """This allows us to map an entire pandas table, without having to specify individual columns.

    Like specifying columns, this is one of the essential components of Qualipy. Using table ``allows`` us to map
    an entire dataframe at once, automatically generating the metadata for each column.

    Note - You must explicitly add it to the Project object in order for it to run.

    Args:
        columns: Can be either a list of column names you want to map, or "all", in which case
            you must supply a sample_dataset so Qualipy can infer what columns you are using.
        infer_schema: If set to True, Qualipy will infer the column types based on the pandas dtypes plus
            some other rules. Individual columns can be overwritten using the types parameter.
        table_name: Metadata for qualipy reasons. Not necessary.
        overwrite_type: This is useful if the aggregate function requires a specific datatype for it to be
            computed.
        ignore: List of columns you don't want to map. Usefull if you specified "all" for columns.
        types: Dictionary of column name to Qualipy type
        bool_as_cat: Should boolean columns be interpreted as categorical data
        int_as_cat: if set to True, all integer columns will be interpreted as categories.
            If set to an integer, only integer values with a number of unique values less than
            the set integer will be considered categories
        functions: A list of properly defined functions.
        extra_functions: If this mapping is used for multiple columns but want a function to be applied to
            only one of the columns, use this. See example for more information.
        sample_dataset: A chunk of the data must be supplied for the inference to work.

    Returns:
        A table (merely a collection of qualipy columns) object that can be added to a Project.
        See Project for more details.

    """
    dialect = engine.dialect.name.lower()
    insp = sa.engine.reflection.Inspector.from_engine(engine)
    all_columns = {
        col["name"]: col
        for col in insp.get_columns(table_name, schema=schema)
        if "name" in col
    }
    column_objects = []
    if extra_functions is None:
        extra_functions = {}
    if ignore is None:
        ignore = []
    if types is None:
        types = {}
    if functions is None:
        functions = []
    if columns == "all":
        all_columns = {
            name: refl for name, refl in all_columns.items() if name not in ignore
        }
    else:
        all_columns = {
            name: refl for name, refl in all_columns.items() if name in columns
        }

    for col_name, reflection in all_columns.items():
        if col_name in types:
            col_type = types[col_name]
        elif infer_schema:
            if "int" in str(reflection["type"]).lower() and (
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
        is_cat = isinstance(col_type, pObjectType)
        is_date = isinstance(col_type, pDateTimeType)
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