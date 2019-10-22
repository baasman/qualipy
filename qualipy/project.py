from qualipy.util import HOME, import_function_by_name
from qualipy.database import delete_data, get_project_table
from qualipy._sql import SQLite
from qualipy.column import Column, Table

import json
import os
import datetime
import pandas as pd
from typing import List, Optional, Union, Dict, Callable

from sqlalchemy import engine, create_engine


def _validate_project_name(project_name):
    assert "-" not in project_name


def create_qualipy_folder(config_dir, db_url):
    if not os.path.exists(config_dir):
        os.makedirs(config_dir, exist_ok=True)
        with open(os.path.join(config_dir, "config.json"), "w") as f:
            json.dump({"interval_time": 100000, "db_url": db_url}, f)
        os.makedirs(os.path.join(config_dir, "models"), exist_ok=True)


class Project(object):
    def __init__(
        self,
        project_name: str,
        engine: Optional[engine.base.Engine] = None,
        config_dir: str = None,
    ):
        _validate_project_name(project_name)
        self.project_name = project_name
        self.value_table = "{}_values".format(self.project_name)
        self.value_custom_table = "{}_values_custom".format(self.project_name)
        self.columns = {}
        self.config_dir = (
            os.path.join(HOME, ".qualipy") if config_dir is None else config_dir
        )
        if engine is None:
            self.engine = create_engine(
                "sqlite:///{}".format(os.path.join(self.config_dir, "qualipy.db"))
            )
        else:
            self.engine = engine
        create_qualipy_folder(self.config_dir, db_url=str(self.engine.url))
        self._create_table(self.project_name, SQLite.create_table)
        self._create_table(self.value_table, SQLite.create_value_table)
        self._create_table(self.value_custom_table, SQLite.create_custom_value_table)

    def add_column(self, column: Column) -> None:
        if isinstance(column, list):
            for col in column:
                self._add_column(col)
        else:
            self._add_column(column)

    def add_table(self, table: Table) -> None:
        if table.infer_schema:
            sample_row = table.extract_sample_row()
        else:
            sample_row = None
        table._generate_columns(sample_row, table.infer_schema)
        self.time_column = table.time_column
        for column in table._columns:
            imported_functions = {}
            for function in column.functions:
                imported_functions[function] = table._import_function(function)
            column.functions = imported_functions
            self.columns[column.name] = column._as_dict(
                column.name, read_functions=False
            )

    def _create_table(self, name: str, create_function: Callable):
        with self.engine.connect() as conn:
            exists = conn.execute(
                "select name from sqlite_master "
                'where type="table" '
                'and name="{}"'.format(name)
            ).fetchone()
            if not exists:
                create_function(conn, name)

    def _add_column(self, column: Union[Column, List[Column]]) -> None:
        if isinstance(column.column_name, list):
            for col in column.column_name:
                self.columns[col] = column._as_dict(col)
        else:
            self.columns[column.column_name] = column._as_dict(name=column.column_name)

    def get_project_table(self) -> pd.DataFrame:
        return SQLite.get_project_table(self.engine, self.project_name)

    def delete_data(self):
        with self.engine.begin() as conn:
            SQLite.delete_data(conn, self.project_name, SQLite.create_table)
            SQLite.delete_data(conn, self.value_table, SQLite.create_value_table)
            SQLite.delete_data(
                conn, self.value_custom_table, SQLite.create_custom_value_table
            )

    def delete_from_project_config(self):
        project_file_path = os.path.join(self.config_dir, "projects.json")
        try:
            with open(project_file_path, "r") as f:
                projects = json.loads(f.read())
        except:
            projects = {}
        projects.pop(self.project_name, None)
        with open(project_file_path, "w") as f:
            json.dump(projects, f)

    def add_to_project_list(
        self, schema: Dict[str, Dict[str, Union[str, bool]]], reset_config: bool = False
    ) -> None:
        project_file_path = os.path.join(self.config_dir, "projects.json")
        try:
            with open(project_file_path, "r") as f:
                projects = json.loads(f.read())
        except:
            projects = {}

        if self.project_name not in projects or reset_config:
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
