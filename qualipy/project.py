import json
import os
import datetime
import pandas as pd
from typing import List, Union, Dict
from collections import Callable

from sqlalchemy import create_engine

from qualipy.util import HOME
from qualipy._sql import DB_ENGINES
from qualipy.reflect.column import Column
from qualipy.reflect.table import Table


def _validate_project_name(project_name):
    assert "-" not in project_name


def set_default_config(config_dir, db_url=None):
    conf = {}
    if db_url is None:
        db_url = f'sqlite:///{os.path.join(config_dir, "qualipy.db")}'
    conf["QUALIPY_DB"] = db_url
    return conf


def create_qualipy_folder(config_dir, db_url=None):
    if not os.path.exists(config_dir):
        os.makedirs(config_dir, exist_ok=True)
        with open(os.path.join(config_dir, "config.json"), "w") as f:
            json.dump(set_default_config(config_dir, db_url), f)
        with open(os.path.join(config_dir, "projects.json"), "w") as f:
            json.dump({}, f)
        os.makedirs(os.path.join(config_dir, "models"), exist_ok=True)


def inspect_db_connection(url):
    for backend in DB_ENGINES.keys():
        if backend in url:
            return backend


class Project(object):
    def __init__(self, project_name: str, config_dir: str, re_init=False):
        self._initialize(
            project_name=project_name, config_dir=config_dir, re_init=re_init
        )

    def _initialize(
        self, project_name: str, config_dir: str, re_init: bool = False,
    ):
        self.project_name = project_name
        self.value_table = "{}_values".format(self.project_name)
        self.value_custom_table = "{}_values_custom".format(self.project_name)
        self.anomaly_table = "{}_anomaly".format(self.project_name)
        if not re_init:
            self.columns = {}
        self.config_dir = config_dir
        with open(os.path.join(self.config_dir, "config.json"), "rb") as f:
            config = json.load(f)
        engine = config.get("QUALIPY_DB")
        self.engine = create_engine(engine)
        self.db_schema = config.get("SCHEMA")
        self.sql_helper = DB_ENGINES[inspect_db_connection(str(self.engine.url))](
            self.db_schema
        )
        if re_init:
            exists = self.sql_helper.does_table_exist(self.engine, self.project_name)
            if exists is None:
                raise Exception(f"Project {project_name} does not exist.")

        if not re_init:
            self.sql_helper.create_schema_if_not_exists(self.engine)
            self.sql_helper.create_table(self.engine, self.project_name)
            self.sql_helper.create_anomaly_table(self.engine, self.anomaly_table)

    def change_config_dir(self, config_dir):
        self._initialize(self.project_name, config_dir, True)

    def add_column(self, column: Column, name: str = None) -> None:
        if isinstance(column, list):
            for col in column:
                self._add_column(col)
        else:
            if isinstance(column, Callable):
                self._add_column_func(column, name)
            else:
                self._add_column_class(column, name)

    def add_table(self, table: Table, extract_sample=False) -> None:
        table._generate_columns(extract_sample=extract_sample)
        self.time_column = table.time_column
        for column in table._columns:
            imported_functions = []
            for function in column.functions:
                imported_functions.append((function, table._import_function(function)))
            column.functions = imported_functions
            self.columns[column.column_name] = column._as_dict(
                column.column_name, read_functions=False
            )

    def _add_column_class(
        self, column: Union[Column, List[Column]], name: str = None
    ) -> None:
        if name is None:
            name = column.column_name
        if isinstance(name, list):
            for n in name:
                self.columns[n] = column._as_dict(name=n)
        else:
            self.columns[name] = column._as_dict(name=name)

    def _add_column_func(
        self, column: Union[Column, List[Column]], name: str = None
    ) -> None:
        if name is None:
            name = column.column_name
        if isinstance(name, list):
            for n in name:
                self.columns[n] = column(name=n)
        else:
            self.columns[name] = column(name)

    def get_project_table(self) -> pd.DataFrame:
        return self.sql_helper.get_project_table(self.engine, self.project_name)

    def get_anomaly_table(self) -> pd.DataFrame:
        return self.sql_helper.get_anomaly_table(self.engine, self.project_name)

    def delete_data(self, recreate=True):
        self.sql_helper.delete_data(
            engine=self.engine,
            name=self.project_name,
            anomaly_name=self.anomaly_table,
            recreate=recreate,
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
