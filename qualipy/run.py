import pandas as pd
import numpy as np

import os
import datetime
from typing import Any, Dict, Optional, Union, Dict, List, Callable
import warnings

from qualipy.backends.pandas_backend.generator import BackendPandas
from qualipy.exceptions import FailException, NullableError
from qualipy.config import STANDARD_VIZ_STATIC, STANDARD_VIZ_DYNAMIC
from qualipy.project import Project

try:
    from qualipy.backends.spark_backend.generator import BackendSpark
except Exception as e:
    print(e)
    warnings.warn("Unable to import pyspark")
    BackendSpark = None


HOME = os.path.expanduser("~")


GENERATORS = {"pandas": BackendPandas, 'spark': BackendSpark}

# types
Measure = List[Dict[str, Any]]


def _create_value(
    value: Any,
    metric: str,
    name: str,
    date: datetime.datetime,
    type: str,
    return_format: str,
):
    return {
        "value": value,
        "date": date,
        "column_name": name,
        "metric": metric,
        "standard_viz": np.NaN,
        "is_static": True,
        "type": type,
        "return_format": return_format,
        "key_function": False,
    }


def set_standard_viz_params(
    function_name: str,
    viz_options_static: Dict[str, Dict[str, str]],
    viz_options_dynamic: Dict[str, Dict[str, str]],
):
    if function_name in viz_options_static:
        standard_viz = viz_options_static[function_name]["function"]
        is_static = True
    elif function_name in viz_options_dynamic:
        standard_viz = viz_options_dynamic[function_name]["function"]
        is_static = False
    else:
        standard_viz = np.NaN
        is_static = True
    return standard_viz, is_static


class DataSet(object):
    def __init__(
        self,
        project: Project,
        backend: str = "pandas",
        time_of_run: Optional[datetime.datetime] = None,
        batch_name: str = None,
        train_anomaly: bool = False,
    ):
        self.project = project
        self.time_of_run = (
            datetime.datetime.now() if time_of_run is None else time_of_run
        )
        self.batch_name = batch_name if batch_name is not None else self.time_of_run

        self.current_data = None
        self.total_measures = []
        self.generator = GENERATORS[backend](project.config_dir)

        self.train_anomaly = train_anomaly
        self.chunk = False
        self.run_n = 0
        self.schema = {}
        self.from_step = None

    def run(self, autocommit: bool = False) -> None:
        if not self.chunk:
            self._generate_metrics(autocommit=autocommit)
            self.run_n += 1
        else:
            for chunk in self.time_chunks:
                print(f"Running on chunk: {chunk['batch_name']}")
                self.current_data = chunk["chunk"]
                if self.current_data.shape[0] > 0:
                    self.batch_name = str(chunk["batch_name"])
                    self.time_of_run = chunk["batch_name"]
                    self._generate_metrics(autocommit=autocommit)
                else:
                    print(f"No data found for {chunk['batch_name']}")

    def set_dataset(
        self, df, columns: Optional[List[str]] = None, name: str = None
    ) -> None:
        self.current_data = df
        self.current_name = name if name is not None else self.run_n
        self.columns = self._set_columns(columns)
        self._set_schema(df)

    def set_chunked_dataset(
        self,
        df,
        columns: Optional[List[str]] = None,
        name: str = None,
        time_freq: str = "1D",
        time_column=None,
    ):
        self.current_data = df
        self.current_name = name if name is not None else self.run_n
        self.columns = self._set_columns(columns)
        self._set_schema(df)
        self.chunk = True
        time_column = (
            time_column if time_column is not None else self.project.time_column
        )
        self.time_chunks = self.generator.get_chunks(df, time_freq, time_column)

    def _set_schema(self, df):
        schema = self.generator.set_schema(df, self.columns, self.current_name)
        self.schema = {**self.schema, **schema}

    def _set_columns(self, columns: Optional[List[str]]):
        if columns is None:
            columns = self.project.columns
        else:
            columns = {
                col: items
                for col, items in self.project.columns.items()
                if col in columns
            }
        return columns

    def commit(self):
        with self.project.engine.begin() as conn:
            self._write(conn=conn, measures=self.total_measures)

    def _generate_metrics(self, autocommit: bool = True) -> None:
        measures = []
        types = {float: "float", int: "int", bool: "bool", dict: "dict", str: "str"}
        for col, specs in self.project.columns.items():

            # TODO: cant be based on column name - should be on project column name
            if col not in self.columns:
                continue

            column_name = specs["name"]

            # enforce type for function
            self.generator.check_type(
                data=self.current_data,
                column=column_name,
                desired_type=specs["type"],
                force=specs["force_type"],
            )
            overwrite_type = specs["overwrite_type"]
            if overwrite_type:
                self.current_data = self.generator.overwrite_type(
                    self.current_data, column_name, specs["type"]
                )

            # get default column info
            measures = self._get_column_specific_general_info(specs, measures)

            # run through functions for column, if any
            # TODO: this breaks if same function name diff arguments
            for function_name, function in {
                **specs["functions"],
                **specs["extra_functions"],
            }.items():

                standard_viz, is_static = set_standard_viz_params(
                    function_name, STANDARD_VIZ_STATIC, STANDARD_VIZ_DYNAMIC
                )

                should_fail = function.fail
                arguments = function.arguments
                return_format = function.return_format
                is_key_function = function.key_function
                return_format_repr = types[return_format]
                viz_type = self._set_viz_type(function, function_name)

                # generate result row
                result = self.generator.generate_description(
                    function=function,
                    data=self.current_data,
                    column=column_name,
                    standard_viz=standard_viz,
                    function_name=function_name,
                    date=self.time_of_run,
                    is_static=is_static,
                    viz_type=viz_type,
                    return_format=return_format_repr,
                    key_function=is_key_function,
                    kwargs=arguments,
                )

                # set value type
                result["value"] = self.generator.set_return_value_type(
                    value=result["value"], return_format=return_format
                )

                if should_fail and not result["value"]:
                    raise FailException(
                        "Program halted by function '{}' for variable '{}' with "
                        "parameter 'fail=True'".format(function_name, col)
                    )

                measures.append(result)

        measures = self._get_general_info(measures)
        measures = [{**m, **{"run_name": self.current_name}} for m in measures]
        self._add_to_total_measures(measures)
        if autocommit:
            self.commit()

    def _add_to_total_measures(self, measures: List[Dict]):
        self.total_measures.extend(measures)

    def _get_column_specific_general_info(self, specs, measures: Measure):
        col_name = specs["name"]
        unique, perc_missing, value_props = self.generator.generate_column_general_info(
            specs, self.current_data, self.time_of_run
        )
        if unique is not None:
            measures.append(unique)
        if value_props is not None:
            measures.append(value_props)
        measures.append(perc_missing)

        if perc_missing["value"] > 0 and specs["force_null"] and not specs["null"]:
            raise NullableError(
                "Column {} has {} percent missing even"
                " though it is not nullable".format(col_name, perc_missing["value"])
            )
        measures.append(
            _create_value(
                str(self.generator.get_dtype(self.current_data, col_name)),
                "dtype",
                col_name,
                self.time_of_run,
                "data-characteristic",
                "str",
            )
        )
        return measures

    def _get_general_info(self, measures: Measure) -> Measure:
        rows, cols = self.generator.get_shape(self.current_data)
        measures.append(
            _create_value(
                rows, "count", "rows", self.time_of_run, "data-characteristic", "int"
            )
        )
        measures.append(
            _create_value(
                cols, "count", "columns", self.time_of_run, "data-characteristic", "int"
            )
        )
        return measures

    def _set_viz_type(self, function: Callable, function_name: str) -> str:
        return_format = function.return_format
        types = {
            float: "numerical",
            int: "numerical",
            bool: "boolean",
            dict: "categorical",
            str: "not_sure",
        }
        viz_type = types[return_format]
        return viz_type

    def _write(self, conn, measures: Measure) -> None:
        if self.chunk:
            batch_name = 'from_chunked'
        else:
            batch_name = self.batch_name
        self.generator.write(
            conn, measures, self.project, batch_name, schema=self.project.db_schema
        )
        self.project.add_to_project_list(self.schema)
