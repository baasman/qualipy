# TODO: remove this once i have it working
import findspark

findspark.init()

from qualipy.backends.base import BackendBase
from qualipy.exceptions import InvalidType, InvalidReturnValue
from qualipy.column import function
from qualipy.backends.spark_backend.functions import is_unique, percentage_missing

import pyspark
from numpy import NaN

from typing import Tuple, Dict, Union, Callable, List, Optional, Any
import datetime
import warnings


DataFrame = pyspark.sql.dataframe.DataFrame
Column = Dict[str, Union[str, bool, Dict[str, Callable]]]


def _create_arg_string(
    keyword_arguments: Dict[str, Any], other_columns: Optional[List[str]] = None
) -> str:
    if keyword_arguments:
        if other_columns is not None:
            col_arguments = {
                "col_{}".format(idx): k for idx, k in enumerate(other_columns)
            }
            for col in other_columns:
                keyword_arguments.pop(col)
            keyword_arguments = {**keyword_arguments, **col_arguments}
        return str(keyword_arguments)
    return NaN


class BackendSpark(BackendBase):
    @staticmethod
    def get_shape(data: DataFrame) -> Tuple[int, int]:
        rows = data.count()
        columns = len(data.columns)
        return rows, columns

    @staticmethod
    def get_dtype(data: DataFrame, column: str) -> str:
        return data.schema[column].dataType.simpleString()

    @staticmethod
    def set_schema(
        data: DataFrame, columns: Dict[str, Column]
    ) -> Dict[str, Union[bool, str]]:
        schema = {
            col: {
                "nullable": info["null"],
                "unique": info["unique"],
                "dtype": BackendSpark.get_dtype(data, col),
            }
            for col, info in columns.items()
        }
        return schema

    @staticmethod
    def set_return_value_type(value: type, return_format: type):
        if return_format in [int, float, str, dict, bool]:
            try:
                value = return_format(value)
            except TypeError as e:
                raise InvalidReturnValue(
                    "Invalid return value: {}, was expecting"
                    " '{}'".format(e, str(return_format))
                )
        elif return_format == "custom":
            pass
        else:
            raise InvalidReturnValue(
                "Unsupported type: '{}'".format(str(return_format))
            )
        return value

    @staticmethod
    def get_other_columns(
        other_column: Optional[Union[List[str], str]],
        arguments: Dict[str, Any],
        data: DataFrame,
    ):
        pass

    @staticmethod
    def generate_description(
        function: Callable,
        data: DataFrame,
        column: str,
        date: datetime.datetime,
        function_name: str,
        standard_viz: bool,
        is_static: bool = True,
        other_columns: Optional[Dict[str, Any]] = None,
        viz_type: str = "numerical",
        kwargs: Dict[str, Any] = None,
    ):
        kwargs = {} if kwargs is None else kwargs
        if other_columns is not None:
            kwargs = {**kwargs, **other_columns}
        value = function(data, column, **kwargs)
        return {
            "value": value,
            "metric": function_name,
            "arguments": _create_arg_string(kwargs, other_columns),
            "date": date,
            "column_name": column,
            "standard_viz": standard_viz,
            "is_static": is_static,
            "type": viz_type,
        }

    @staticmethod
    def check_type(data, column, desired_type, force=False):
        pass

    @staticmethod
    def generate_column_general_info(specs, data, time_of_run):
        col_name = specs["name"]
        if specs["unique"]:
            unique = BackendSpark.generate_description(
                function=is_unique,
                data=data,
                column=col_name,
                standard_viz=NaN,
                function_name="is_unique",
                date=time_of_run,
                is_static=True,
                viz_type="data-characteristic",
                kwargs={},
            )
        else:
            unique = None
        perc_missing = BackendSpark.generate_description(
            function=percentage_missing,
            data=data,
            column=col_name,
            standard_viz=NaN,
            function_name="perc_missing",
            date=time_of_run,
            is_static=True,
            viz_type="data-characteristic",
            kwargs={},
        )
        return unique, perc_missing


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F

    spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    df = spark.read.format("csv").load(
        "/Users/baasman/PycharmProjects/qualipy/examples/iris.csv",
        header="true",
        inferSchema="true",
    )

    BackendSpark.get_shape(df)
    BackendSpark.get_dtype(df, "sepal_length")

    cols = {
        "sepal_length": {
            "name": "sepal_length",
            "type": "double",
            "force_type": True,
            "null": True,
            "force_null": False,
            "unique": False,
            "functions": {},
        }
    }
    BackendSpark.set_schema(df, cols)

    @function(return_format=float)
    def greater_than_3(data, column):
        return data.filter(F.col(column) > 5).count()

    val = BackendSpark.generate_description(
        greater_than_3, df, "sepal_length", "now", "greater_than", False
    )
