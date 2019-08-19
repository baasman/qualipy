from qualipy.util import HOME
from qualipy.database import (
    delete_data,
    create_table,
    create_alert_table,
    create_custom_value_table,
    create_value_table,
    get_all_values,
    create_table_if_not_exists,
)
from qualipy.column import Column

import json
import os
import datetime
import pickle
import pandas as pd
from typing import List, Optional, Union, Dict, Callable

from sqlalchemy import engine, create_engine


class Project(object):
    def __init__(
        self,
        project_name: str,
        engine: Optional[engine.base.Engine] = None,
        reset_config: bool = False,
        config_dir: str = None,
    ):
        self.project_name = project_name
        self.alert_table_name = "{}_alerts".format(self.project_name)
        self.value_table = "{}_values".format(self.project_name)
        self.value_custom_table = "{}_values_custom".format(self.project_name)
        self.columns = {}
        self.reset_config = reset_config
        self.config_dir = (
            os.path.join(HOME, ".qualipy") if config_dir is None else config_dir
        )
        if engine is None:
            self.engine = create_engine(
                "sqlite:///{}".format(os.path.join(HOME, ".qualipy", "qualipy.db"))
            )
        else:
            self.engine = engine
        self._create_table(self.project_name, create_table)
        self._create_table(self.value_table, create_value_table)
        self._create_table(self.value_custom_table, create_custom_value_table)
        self._create_table(self.alert_table_name, create_alert_table)

    def add_column(self, column: Column) -> None:
        if isinstance(column, list):
            for col in column:
                self._add_column(col)
        else:
            self._add_column(column)

    def _create_table(self, name: str, create_function: Callable):
        with self.engine.connect() as conn:
            exists = conn.execute(
                "select name from sqlite_master "
                'where type="table" '
                'and name="{}"'.format(name)
            ).fetchone()
            if not exists:
                create_function(self.engine, name)

    def _add_column(self, column: Union[Column, List[Column]]) -> None:
        if isinstance(column.column_name, list):
            for col in column.column_name:
                self.columns[col] = column._as_dict(col)
        else:
            self.columns[column.column_name] = column._as_dict(name=column.column_name)

    def get_project_table(self) -> pd.DataFrame:
        data = get_all_values(self.engine, self.project_name)
        data = data.drop("valueID", axis=1)
        return data

    def delete_data(self):
        with self.engine.begin() as conn:
            delete_data(conn, self.project_name, create_table)
            delete_data(conn, self.value_table, create_value_table)
            delete_data(conn, self.value_custom_table, create_custom_value_table)
            delete_data(conn, self.alert_table_name, create_alert_table)

    def delete_from_project_list(self):
        pass

    def add_to_project_list(self, schema: Dict[str, str]) -> None:
        project_file_path = os.path.join(self.config_dir, "projects.json")
        try:
            with open(project_file_path, "r") as f:
                projects = json.loads(f.read())
        except:
            projects = {}

        if self.project_name not in projects or self.reset_config:
            projects[self.project_name] = {
                "columns": list(self.columns.keys()),
                "executions": [datetime.datetime.now().strftime("%m/%d/%Y %H:%M")],
                "db": str(self.engine.url),
                "schema": schema,
            }
        else:
            projects[self.project_name]["executions"].append(
                str(datetime.datetime.now())
            )
        with open(project_file_path, "w") as f:
            json.dump(projects, f)
