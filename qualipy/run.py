import pandas as pd
import numpy as np

import os
import datetime
import pickle
from typing import Any, Dict, Optional, Union, Dict, List

from qualipy.backends.pandas_backend.generator import BackendPandas
from qualipy.backends.pandas_backend.functions import is_unique, percentage_missing
from qualipy.database import create_table, get_table, create_alert_table
from qualipy.anomaly_detection import find_anomalies_by_std
from qualipy.exceptions import FailException, NullableError
from qualipy.config import (
    OVERVIEW_PAGE_COLUMNS,
    OVERVIEW_PAGE_METRICS_DEFAULT,
    STANDARD_VIZ_STATIC,
    STANDARD_VIZ_DYNAMIC,
)
from qualipy.backends.pandas_backend.pandas_types import (
    DateTimeType,
    FloatType,
    IntType,
    ObjectType,
)
from qualipy.project import Project


HOME = os.path.expanduser("~")


GENERATORS = {"pandas": BackendPandas}

AllowedTypes = Union[DateTimeType, FloatType, IntType, ObjectType]


def _create_value(
    value: Any, metric: str, name: str, date: datetime.datetime, type: str
):
    return {
        "value": value,
        "date": date,
        "column_name": name,
        "metric": metric,
        "standard_viz": np.NaN,
        "is_static": True,
        "type": type,
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
        reset: bool = False,
        time_of_run: Optional[datetime.datetime] = None,
        batch_name: str = None,
    ):
        self.project = project
        self.alert_table_name = "{}_alerts".format(project.project_name)
        self.time_of_run = (
            datetime.datetime.now() if time_of_run is None else time_of_run
        )
        self.batch_name = batch_name if batch_name is not None else self.time_of_run

        self.current_data = None
        self.reset = reset
        self.generator = GENERATORS[backend]()

        self._locate_history_data()
        self._get_alerts()

    def run(self) -> None:
        self._generate_metrics()

    def set_dataset(self, df: pd.DataFrame) -> None:
        self.current_data = df
        self.schema = self.generator.set_schema(df, self.project.columns)

    def _locate_history_data(self) -> pd.DataFrame:
        with self.project.engine.connect() as conn:
            exists = conn.execute(
                "select name from sqlite_master "
                'where type="table" '
                'and name="{}"'.format(self.project.project_name)
            ).fetchone()
            if not exists:
                create_table(self.project.engine, self.project.project_name)
            if self.reset:
                try:
                    conn.execute("drop table {}".format(self.project.project_name))
                except:
                    pass
                create_table(self.project.engine, self.project.project_name)
                conn.execute("delete from {}".format(self.project.project_name))

            hist_data = get_table(
                engine=self.project.engine, table_name=self.project.project_name
            )
        return hist_data

    def _generate_metrics(self) -> None:
        measures = []
        anomalies = []
        for col, specs in self.project.columns.items():

            # enforce type for function
            self.generator.check_type(
                data=self.current_data,
                column=col,
                desired_type=specs["type"],
                force=specs["force_type"],
            )

            # run through functions for column
            for function_name, function in specs["functions"].items():

                standard_viz, is_static = set_standard_viz_params(
                    function_name, STANDARD_VIZ_STATIC, STANDARD_VIZ_DYNAMIC
                )

                should_fail = function.fail
                arguments = function.arguments
                other_columns = self.generator.get_other_columns(
                    other_column=function.other_column,
                    arguments=arguments,
                    data=self.current_data,
                )
                viz_type = self._set_viz_type(function, function_name)

                # generate result row
                result = self.generator.generate_description(
                    function=function,
                    data=self.current_data,
                    column=col,
                    standard_viz=standard_viz,
                    function_name=function_name,
                    date=self.time_of_run,
                    other_columns=other_columns,
                    is_static=is_static,
                    viz_type=viz_type,
                    kwargs=arguments,
                )

                # set value type
                result["value"] = self.generator.set_return_value_type(
                    value=result["value"], return_format=function.return_format
                )

                if should_fail and not result["value"]:
                    raise FailException(
                        "Program halted by function '{}' for variable '{}' with "
                        "parameter 'fail=True'".format(function_name, col)
                    )

                measures.append(result)
                measures = self._get_column_specific_general_info(specs, measures)

        self.current_run = pd.DataFrame(measures)
        for col, specs in self.project.columns.items():
            for function_name, function in specs["functions"].items():
                if function.anomaly:
                    anomalies.append(
                        self.get_alerts(
                            column_name=col, function_name=function_name, std_away=2
                        )
                    )

        measures = self._get_general_info(measures)
        self._write(measures)

    def _get_column_specific_general_info(self, specs, measures):
        col_name = specs["name"]
        if specs["unique"]:
            measures.append(
                self.generator.generate_description(
                    function=is_unique,
                    data=self.current_data,
                    column=col_name,
                    standard_viz=np.NaN,
                    function_name="is_unique",
                    date=self.time_of_run,
                    is_static=True,
                    viz_type="data-characteristic",
                    kwargs={},
                )
            )
        perc_missing = self.generator.generate_description(
            function=percentage_missing,
            data=self.current_data,
            column=col_name,
            standard_viz=np.NaN,
            function_name="perc_missing",
            date=self.time_of_run,
            is_static=True,
            viz_type="data-characteristic",
            kwargs={},
        )
        if perc_missing["value"] > 0 and specs["force_null"] and not specs["null"]:
            raise NullableError(
                "Column {} has {} percent missing even"
                " though it is not nullable".format(
                    specs["column_name"], perc_missing["value"]
                )
            )
        measures.append(perc_missing)
        measures.append(
            _create_value(
                str(self.generator.get_dtype(self.current_data, col_name)),
                "dtype",
                col_name,
                self.time_of_run,
                "data-characteristic",
            )
        )
        return measures

    def _get_general_info(self, measures: List[Dict[str, Any]]):
        rows, cols = self.generator.get_shape(self.current_data)
        measures.append(
            _create_value(
                rows, "count", "rows", self.time_of_run, "data-characteristic"
            )
        )
        measures.append(
            _create_value(
                cols, "count", "columns", self.time_of_run, "data-characteristic"
            )
        )
        return measures

    def _set_viz_type(self, function, function_name):
        return_format = function.return_format
        types = {
            float: "numerical",
            int: "numerical",
            bool: "boolean",
            dict: "custom",
            str: "not_sure",
        }
        viz_type = types[return_format]
        if viz_type == "custom":
            if function_name in list(STANDARD_VIZ_STATIC.keys()):
                viz_type = "standard_viz_static"
            elif function_name in list(STANDARD_VIZ_DYNAMIC.keys()):
                viz_type = "standard_viz_dynamic"
        return viz_type

    def _write(self, measures):
        data = pd.DataFrame(measures)
        # data = self.set_additional_viz_type(data)
        data["batch_name"] = self.batch_name
        self.current_data_measures = data.copy()

        # all values are getting binary data for now, need to think of solution for this
        data.value = data.value.apply(lambda v: pickle.dumps(v))
        data.to_sql(
            self.project.project_name,
            self.project.engine,
            if_exists="append",
            index=False,
        )

        self.project.add_to_project_list(self.schema)

    def _get_alerts(self):
        with self.project.engine.connect() as conn:
            exists = conn.execute(
                "select name from sqlite_master "
                'where type="table" '
                'and name="{}"'.format(self.alert_table_name)
            ).fetchone()
            if not exists:
                create_alert_table(self.project.engine, self.alert_table_name)
            if self.reset:
                try:
                    conn.execute("drop table {}".format(self.alert_table_name))
                except:
                    pass
                create_alert_table(self.project.engine, self.alert_table_name)
                conn.execute("delete from {}".format(self.alert_table_name))

            self.alert_data = get_table(
                engine=self.project.engine, table_name=self.alert_table_name
            )

    def get_alerts(self, column_name, function_name, std_away=3):

        hist_data = self._locate_history_data()
        hist_data.value = hist_data.value.apply(lambda r: pickle.loads(r))

        nrows = hist_data[
            (hist_data["column_name"] == column_name)
            & (hist_data["metric"] == function_name)
        ].shape[0]

        if nrows < 1000:
            print("Not enough batches to accurately determine outliers")
            return

        is_anomaly, value = find_anomalies_by_std(
            self.current_data, hist_data, column_name, column_name, std_away
        )
        if is_anomaly:
            return {
                "column": column_name,
                "std_away": std_away,
                "value": value,
                "date": self.time_of_run,
                "alert_message": "This value is more than {} standard deviations away".format(
                    std_away
                ),
            }
        return
