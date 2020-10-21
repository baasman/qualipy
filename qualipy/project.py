import json
import os
import datetime
import pandas as pd
from typing import List, Union, Dict

try:
    from collections.abc import Callable
except ImportError:
    from collections import Callable

from sqlalchemy import create_engine

from qualipy.util import HOME
from qualipy._sql import DB_ENGINES
from qualipy.reflect.column import Column
from qualipy.reflect.table import Table
from qualipy._schema import config_schema


def _validate_project_name(project_name):
    assert "-" not in project_name


def set_default_config(config_dir, db_url=None):
    conf = {}
    if db_url is None:
        db_url = f'sqlite:///{os.path.join(config_dir, "qualipy.db")}'
    conf["QUALIPY_DB"] = db_url
    return conf


def generate_config(config_dir, db_url=None):
    config_dir = os.path.expanduser(config_dir)
    if not os.path.exists(config_dir):
        os.makedirs(config_dir, exist_ok=True)
        with open(os.path.join(config_dir, "config.json"), "w") as f:
            json.dump(set_default_config(config_dir, db_url), f)
        with open(os.path.join(config_dir, "projects.json"), "w") as f:
            json.dump({}, f)
        os.makedirs(os.path.join(config_dir, "models"), exist_ok=True)
        os.makedirs(os.path.join(config_dir, "profile_data"), exist_ok=True)
        os.makedirs(os.path.join(config_dir, "reports"), exist_ok=True)
        os.makedirs(os.path.join(config_dir, "reports", "anomaly"), exist_ok=True)
        os.makedirs(os.path.join(config_dir, "reports", "profiler"), exist_ok=True)
        os.makedirs(os.path.join(config_dir, "reports", "comparison"), exist_ok=True)


def inspect_db_connection(url):
    for backend in DB_ENGINES.keys():
        if backend in url:
            return backend


class Project(object):
    """The project class points to a specific configuration, and holds all mappings.

    It also includes a lot of useful utility functions for working with the management
    of projects
    """

    def __init__(self, project_name: str, config_dir: str, re_init: bool = False):
        """
        Args:
            project_name: The name of the project. This will be important for referencing
                in report generation later. The project_name can not be changed - as it used
                internally when storing data
            config_dir: A path to the configuration directory, as created using the CLI command ``qualipy generate-config``.
                See the (link here)``config`` section for more information
        """
        self._initialize(
            project_name=project_name, config_dir=config_dir, re_init=re_init
        )

    def _initialize(
        self,
        project_name: str,
        config_dir: str,
        re_init: bool = False,
    ):
        self.project_name = project_name
        self.value_table = "{}_values".format(self.project_name)
        self.value_custom_table = "{}_values_custom".format(self.project_name)
        self.anomaly_table = "{}_anomaly".format(self.project_name)
        if not re_init:
            self.columns = {}
        config_dir = os.path.expanduser(config_dir)
        self.config_dir = config_dir
        if not os.path.isdir(config_dir):
            raise Exception(
                f"""Directory {config_dir} does not exist. 
                \nRun 'qualipy generate-config' before instantiating a project."""
            )
        self._write_default_config_if_not_exists()
        with open(os.path.join(self.config_dir, "config.json"), "rb") as f:
            config = json.load(f)
        self._validate_schema(config)
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

        if not re_init:
            self._functions_used_in_project = {}
        else:
            pass  # read from config

    def change_config_dir(self, config_dir):
        self._initialize(self.project_name, config_dir, True)

    def add_column(self, column: Column, name: str = None) -> None:
        """Add a mapping to this project

        This is the method to use when adding a column mapping to the project. Once added,
        it will automatically be executed when running the pipeline.

        Args:
            column: The column object. Can either be created through the function method or class method.
            name: This is useful when you don't want to run all mappings at once. Often, you'll do analysis
                on different subsets of the same dataset. Use name to reference it later on and only execute
                it for a specific subset.

                This name is also essential if you want to analyze the same column, but in a different
                subset of the data.

        Returns:
            None

        """
        if isinstance(column, list):
            for col in column:
                self._add_column(col)
                self._get_unique_functions(col)
        else:
            if isinstance(column, Callable):
                self._add_column_func(column, name)
            else:
                self._add_column_class(column, name)
        self._get_unique_functions(name)

    def _get_unique_functions(self, name):
        if name is None:
            for column_name in self.columns:
                column = self.columns[column_name]
                for function in column["functions"] + column["extra_functions"]:
                    if function[0] not in self._functions_used_in_project:
                        self._functions_used_in_project[function[0]] = {
                            "display_name": function[1].display_name,
                            "description": function[1].description,
                        }
        else:
            column = self.columns[name]
            for function in column["functions"] + column["extra_functions"]:
                if function[0] not in self._functions_used_in_project:
                    self._functions_used_in_project[function[0]] = {
                        "display_name": function[1].display_name,
                        "description": function[1].description,
                    }

    def _validate_schema(self, config):
        config_schema.validate(config)

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

    def _write_default_config_if_not_exists(self):
        with open(os.path.join(self.config_dir, "config.json"), "rb") as f:
            config = json.load(f)
        if self.project_name not in config:
            config[self.project_name] = {
                "ANOMALY_ARGS": {
                    "check_for_std": False,
                    "importance_level": 1,
                    "distance_from_bound": 1,
                },
                "ANOMALY_MODEL": "prophet",
                "DATE_FORMAT": "%Y-%m-%d",
                "NUM_SEVERITY_LEVEL": 1,
                "CAT_SEVERITY_LEVEL": 1,
                "VISUALIZATION": {
                    "row_counts": {
                        "include_bar_of_latest": {
                            "use": True,
                            "diff": False,
                            "show_by_default": True,
                        },
                        "include_summary": {"use": True, "show_by_default": True},
                    },
                    "trend": {
                        "include_bar_of_latest": {
                            "use": True,
                            "diff": False,
                            "show_by_default": True,
                        },
                        "include_summary": {"use": True, "show_by_default": True},
                        "sst": 3,
                        "point": True,
                        "n_steps": 10,
                        "add_diff": {"shift": 1},
                    },
                    "missing": {
                        "include_0": True,
                        "include_bar_of_latest": {"use": True, "diff": False},
                    },
                },
            }
        with open(os.path.join(self.config_dir, "config.json"), "w") as f:
            json.dump(config, f)

    def write_functions_to_config(self):
        config_file_path = os.path.join(self.config_dir, "config.json")
        with open(config_file_path, "r") as f:
            config = json.loads(f.read())
        if "DISPLAY_NAMES" not in config[self.project_name]:
            config[self.project_name]["DISPLAY_NAMES"] = {}
        config[self.project_name]["DISPLAY_NAMES"][
            "DEFAULT"
        ] = self._functions_used_in_project
        with open(config_file_path, "w") as f:
            json.dump(config, f)

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
