from qualipy.util import HOME
from qualipy._sql import SQLite
from qualipy.column import Column, Table

import json
import os
import datetime
import pandas as pd
from typing import List, Optional, Union, Dict

from sqlalchemy import create_engine


def _validate_project_name(project_name):
    assert "-" not in project_name


def set_default_config():
    return {}


def create_qualipy_folder(config_dir, db_url):
    if not os.path.exists(config_dir):
        os.makedirs(config_dir, exist_ok=True)
        with open(os.path.join(config_dir, "config.json"), "w") as f:
            json.dump(set_default_config(), f)
        with open(os.path.join(config_dir, "projects.json"), "w") as f:
            json.dump({}, f)
        os.makedirs(os.path.join(config_dir, "models"), exist_ok=True)


class Project(object):
    def __init__(
        self, project_name: str, config_dir: str = None,
    ):
        self._initialize(project_name, config_dir, False)

    def _initialize(
        self, project_name: str, config_dir: str = None, re_init: bool = False,
    ):
        _validate_project_name(project_name)
        self.project_name = project_name
        self.value_table = "{}_values".format(self.project_name)
        self.value_custom_table = "{}_values_custom".format(self.project_name)
        self.anomaly_table = "{}_anomaly".format(self.project_name)
        if not re_init:
            self.columns = {}
        if not re_init:
            self.config_dir = (
                os.path.join(HOME, ".qualipy") if config_dir is None else config_dir
            )
        else:
            self.config_dir = config_dir
        with open(os.path.join(self.config_dir, "config.json"), "rb") as f:
            config = json.load(f)
        engine = config.get("QUALIPY_DB")
        if engine is None:
            self.engine = create_engine(
                "sqlite:///{}".format(os.path.join(self.config_dir, "qualipy.db"))
            )
        else:
            self.engine = create_engine(engine)

        if not re_init:
            create_qualipy_folder(self.config_dir, db_url=str(self.engine.url))

        if not re_init:
            SQLite.create_table(self.engine, self.project_name)
            SQLite.create_value_table(self.engine, self.value_table)
            SQLite.create_custom_value_table(self.engine, self.value_custom_table)
            SQLite.create_anomaly_table(self.engine, self.anomaly_table)

    def change_config_dir(self, config_dir):
        # TODO: engine should come from config - never given
        self._initialize(self.project_name, None, config_dir, True)

    def add_column(self, column: Column, name: str = None) -> None:
        if isinstance(column, list):
            for col in column:
                self._add_column(col)
        else:
            self._add_column(column, name)

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

    def _add_column(
        self, column: Union[Column, List[Column]], name: str = None
    ) -> None:
        if name is None:
            name = column.column_name
        self.columns[name] = column._as_dict(name=name)

    def get_project_table(self) -> pd.DataFrame:
        return SQLite.get_project_table(self.engine, self.project_name)

    def get_anomaly_table(self) -> pd.DataFrame:
        return SQLite.get_anomaly_table(self.engine, self.project_name)

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
                "executions": [datetime.datetime.now().strftime("%m/%d/%Y %H:%M")],
                "schema": schema,
            }
        else:
            projects[self.project_name]["executions"].append(
                str(datetime.datetime.now())
            )
            projects[self.project_name]["schema"] = schema
        with open(project_file_path, "w") as f:
            json.dump(projects, f)
