import pandas as pd
import numpy as np

import os
import datetime
from typing import Any, Dict, Optional, Union, Dict, List, Callable
import warnings

from qualipy.backends.pandas_backend.generator import BackendPandas
from qualipy.backends.sql_backend.generator import BackendSQL
from qualipy.exceptions import FailException, NullableError
from qualipy.project import Project

try:
    from qualipy.backends.spark_backend.generator import BackendSpark
except Exception as e:
    BackendSpark = None

# supress numpy future warning for now
warnings.simplefilter(action="ignore", category=FutureWarning)


HOME = os.path.expanduser("~")


GENERATORS = {"pandas": BackendPandas, "spark": BackendSpark, "sql": BackendSQL}

# types
Measure = List[Dict[str, Any]]


def _create_value(
    value: Any,
    metric: str,
    name: str,
    date: datetime.datetime,
    type: str,
    return_format: str,
    run_name,
):
    return {
        "value": value,
        "date": date,
        "column_name": name,
        "metric": metric,
        "type": type,
        "return_format": return_format,
        "run_name": run_name,
    }


class Qualipy(object):
    """
    This is the main entrypoint to Qualipy. This is the object that will actually
    execute on your data.
    """

    def __init__(
        self,
        project: Project,
        backend: str = "pandas",
        time_of_run: Optional[datetime.datetime] = None,
        batch_name: str = None,
    ):
        """
        Args:
            project: Your defined qualipy.Project
            backend: Can be either "pandas", "sql", or "spark" depending on what kind
                of data you are tracking
            time_of_run: If None, this will be the current datetime. Note, this is very important
                for analysis, as time_of_run is essentially your x_axis in all time series analysis.
                Being able to set it to a specific date can be useful when generating retrospective
                statistics.
            batch_name: Useful for comparing specific time points by name during analysis. By default it will
                take the time_of_run as batch_name
        """

        self.project = project
        self.time_of_run = (
            datetime.datetime.now() if time_of_run is None else time_of_run
        )
        self.batch_name = batch_name if batch_name is not None else self.time_of_run

        self.current_data = None
        self.total_measures = []
        self.generator = GENERATORS[backend](project.config_dir)

        self.chunk = False
        self.run_n = 0
        self.schema = {}
        self.from_step = None
        self.stratify = False
        self.backend = backend

    def run(self, autocommit: bool = False, profile_batch=False) -> None:
        """The method that runs the execution

        Note: You must first set a dataset using either ``set_dataset`` or
            ``set_chunked_dataset``

        Args:
            autocommit: If set to True, qualipy will automatically write to it's backend. If set
                to False, the user will have to manually run the ``commit`` function.
            profile_batch: If set to True, Qualipy will generate metadata used to construct
                a batch report by using the ``produce_batch_report`` CLI command.

        Returns:
            None
        """
        if not self.chunk:
            self._run_with_optional_stratify(autocommit, profile_batch=profile_batch)
            self.run_n += 1
        else:
            if profile_batch:
                raise Exception("Can only profile batch without chunking data")
            for chunk in self.time_chunks:
                print(f"Running on chunk: {chunk['batch_name']}")
                self.current_data = chunk["chunk"]
                if self.current_data.shape[0] == 0:
                    self.current_data = self.fallback_data
                self.batch_name = str(chunk["batch_name"])
                self.time_of_run = chunk["batch_name"]
                self._run_with_optional_stratify(autocommit)

    def _run_with_optional_stratify(self, autocommit, profile_batch=False):
        if self.stratify:
            self.original_data = self.current_data.copy()
            self.original_name = self.current_name
            for stratify_value in self.stratify_values:
                self.current_data = self.stratify_function(
                    self.current_data, stratify_value
                )
                self.current_name = f"{self.current_name}_{stratify_value}"
                self._generate_metrics(
                    autocommit=autocommit, profile_batch=profile_batch
                )

                # turn back name and data
                self.current_name = self.original_name
                self.current_data = self.original_data
        else:
            self._generate_metrics(autocommit=autocommit, profile_batch=profile_batch)

    def set_dataset(
        self, df, columns: Optional[List[str]] = None, run_name: str = None
    ) -> None:
        """This specified the exact subset of data you want to run on.

        Use this method when you don't have all of the data (a live process) and want
        to only run on one batch of data.

        Args:
            df: Can be either PandasData, SQLData, or SparkData
            columns: If you don't want to run all mappings on this specific subset
                of data, you can specify just the columns you want to run. Note - this
                corresponds to the ``name`` argument when adding a column to a project
            run_name: If you're running metrics from a project on many different subsets any
                iterations of the data, you might want to give each specific subset a
                name. This is especially necessary when running aggregates on a column
                where the column name itself stays the same, but the meaning changes based
                on the subset. By default, this will take the value of '0'
        Returns:
            None
        """
        # NOTE: if sqldata but pandas backend, should pull data and work on that!
        # also give option of query or taking last x rows
        self._set_data(df, allowed_dataclasses=["SQLData", "PandasData", "SparkData"])
        self.current_name = run_name if run_name is not None else self.run_n
        self._set_stratification(df)
        self.columns = self._set_columns(columns)
        self._set_schema(self.current_data)

    def set_chunked_dataset(
        self,
        df,
        columns: Optional[List[str]] = None,
        run_name: str = None,
        time_freq: str = "1D",
        time_column=None,
    ):
        """This specified the exact subset of data you want to run on.

        Use this method when you already have all data available, and want to retrospectively
        analyze all historical as if it was a live process. Note - There's nothing stopping you
        from first running this on the available data and then running on a batch-per-batch basis
        afterwards using regular ``set_dataset``.

        Args:
            df: Can be either PandasData, SQLData, or SparkData
            columns: If you don't want to run all mappings on this specific subset
                of data, you can specify just the columns you want to run. Note - this
                corresponds to the ``name`` argument when adding a column to a project
            run_name: If you're running metrics from a project on many different subsets any
                iterations of the data, you might want to give each specific subset a
                name. This is especially necessary when running aggregates on a column
                where the column name itself stays the same, but the meaning changes based
                on the subset. By default, this will take the value of '0'
            time_freq: A pandas-like timeseries frequency term. Use this page to know what you
                can use: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases (turn to link)
            time_column: The time series column qualipy should use to chunk the data
        Returns:
            None
        """

        self._set_data(df, allowed_dataclasses=["SQLData", "PandasData", "SparkData"])
        self.current_name = run_name if run_name is not None else self.run_n
        self._set_stratification(df)
        self.columns = self._set_columns(columns)
        self._set_schema(self.current_data)
        self.chunk = True
        try:
            time_column = (
                time_column if time_column is not None else self.project.time_column
            )
        except AttributeError:
            raise Exception(
                "No time_column specified. Must be given if chunking dataset"
            )
        self.time_chunks = self.generator.get_chunks(
            self.current_data, time_freq, time_column
        )

    def _set_data(self, df, allowed_dataclasses):
        if df.__class__.__name__ in allowed_dataclasses:
            self.current_data = df.get_data(backend_used=self.backend)
            try:
                self.fallback_data = df.set_fallback_data()
            except:
                pass
        else:
            raise Exception(f"{df.__class__.__name__} is not yet a supported datatype")

    def _set_stratification(self, df):
        # stratification only implemented in Pandas for now
        if self.backend == "pandas":
            if df.stratify:
                self.stratify = True
                self.stratify_values = df.stratify_values
                self.stratify_function = df.subset_function()

    def _set_schema(self, df):
        schema = self.generator.set_schema(df, self.columns, self.current_name)
        self.schema = {**self.schema, **schema}

    def _set_columns(self, columns: Optional[List[str]]):
        if columns is None:
            ret_columns = self.project.columns
        else:
            # columns = {
            #     col: items
            #     for col, items in self.project.columns.items()
            #     if col in columns
            # }
            ret_columns = {}
            for col, items in self.project.columns.items():
                stage_name = items.get("column_stage_collection_name")
                if stage_name in columns:
                    ret_columns[col] = items
        return ret_columns

    def commit(self):
        with self.project.engine.begin() as conn:
            self._write(conn=conn, measures=self.total_measures)
        self.project.write_functions_to_config()

    def _generate_metrics(
        self, autocommit: bool = True, profile_batch: bool = False
    ) -> None:
        measures = []
        types = {float: "float", int: "int", bool: "bool", dict: "dict", str: "str"}
        for col, specs in self.project.columns.items():

            if col not in self.columns:
                continue

            if specs["split_on"] is not None:
                column_name = specs["name"].split("||")[0]
                self.data_view = self.generator.return_split_subset(
                    self.current_data, specs["split_on"][0], specs["split_on"][1]
                )
                self.current_name_view = f"{self.current_name}-{specs['split_on'][1]}"
            else:
                column_name = specs['name']
                self.data_view = self.generator.return_data_copy(self.current_data)
                self.current_name_view = self.current_name

            # enforce type for function
            # TODO: fix types when sql data is converted to pandas data
            try:
                if specs["type"] is not None:
                    self.generator.check_type(
                        data=self.data_view,
                        column=column_name,
                        desired_type=specs["type"],
                        force=specs["force_type"],
                    )
                overwrite_type = specs["overwrite_type"]
                if overwrite_type:
                    self.data_view = self.generator.overwrite_type(
                        self.data_view, column_name, specs["type"]
                    )
            except AttributeError:
                pass

            # get default column info
            measures = self._get_column_specific_general_info(specs, measures)

            for function_name, function in (
                specs["functions"] + specs["extra_functions"]
            ):

                should_fail = function.fail
                arguments = function.arguments
                return_format = function.return_format
                return_format_repr = types[return_format]
                viz_type = self._set_viz_type(function, function_name)

                # generate result row
                result = self.generator.generate_description(
                    function=function,
                    data=self.data_view,
                    column=column_name,
                    function_name=function_name,
                    date=self.time_of_run,
                    viz_type=viz_type,
                    return_format=return_format_repr,
                    kwargs=arguments,
                )
                result["run_name"] = self.current_name_view

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
        # measures = [{**m, **{"run_name": self.current_name_view}} for m in measures]
        self._add_to_total_measures(measures)
        if profile_batch:
            self.generator.profile_batch(
                self.data_view,
                self.batch_name,
                self.current_name_view,
                self.columns,
                self.project.config_dir,
                self.project.project_name,
            )
        if autocommit:
            self.commit()

    def _add_to_total_measures(self, measures: List[Dict]):
        self.total_measures.extend(measures)

    def _get_column_specific_general_info(self, specs, measures: Measure):
        col_name = specs["name"]
        unique, perc_missing, value_props = self.generator.generate_column_general_info(
            specs, self.data_view, self.time_of_run, self.current_name_view
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
                str(self.generator.get_dtype(self.data_view, col_name)),
                "dtype",
                col_name,
                self.time_of_run,
                "data-characteristic",
                "str",
                self.current_name_view,
            )
        )
        return measures

    def _get_general_info(self, measures: Measure) -> Measure:
        rows, cols = self.generator.get_shape(self.data_view)
        measures.append(
            _create_value(
                rows,
                "count",
                "rows",
                self.time_of_run,
                "data-characteristic",
                "int",
                self.current_name,
            )
        )
        measures.append(
            _create_value(
                cols,
                "count",
                "columns",
                self.time_of_run,
                "data-characteristic",
                "int",
                self.current_name,
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
            batch_name = "from_chunked"
        else:
            batch_name = self.batch_name
        self.generator.write(
            conn, measures, self.project, batch_name, schema=self.project.db_schema
        )
        self.project.add_to_project_list(self.schema)
