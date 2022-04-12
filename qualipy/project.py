import json
import os
import copy
import codecs
import datetime
import pandas as pd
from typing import List, Union, Dict

try:
    from collections.abc import Callable
except ImportError:
    from collections import Callable

from sqlalchemy import create_engine
import dill

from qualipy.util import HOME, copy_function_spec
from qualipy._sql import DB_ENGINES
from qualipy.reflect.column import Column
from qualipy.reflect.table import Table
from qualipy._schema import config_schema
from qualipy.backends.data_types import PANDAS_TYPES
from qualipy.config import QualipyConfig


DATA_TYPES = {"pandas": PANDAS_TYPES, "sql": {}}


def _validate_project_name(project_name):
    assert "-" not in project_name


def inspect_db_connection(url):
    # TODO: should grab dialect instead of this
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
        # need to come up with way that project does not need to redefined and re-imported
        # "QUALIPY_DB": "sqlite:////data/baasman/.omop-prod2/qualipy.db",
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

        self.config = QualipyConfig(
            config_dir=self.config_dir, project_name=project_name
        )
        self.config.set_default_project_config(project_name)
        self.projects = self.config.get_projects()

        engine = self.config["QUALIPY_DB"]
        self.engine = create_engine(engine)
        self.db_schema = self.config.get("SCHEMA")
        self.sql_helper = DB_ENGINES[inspect_db_connection(str(self.engine.url))](
            self.engine, self.db_schema
        )

        if re_init:
            exists = self.sql_helper.does_table_exist(self.project_name)
            if exists is None:
                raise Exception(f"Project {project_name} does not exist.")

        if not re_init:
            self.sql_helper.create_schema_if_not_exists()
            self.sql_helper.create_table(self.project_name)
            self.sql_helper.create_anomaly_table(self.anomaly_table)

        self.sql_helper.reflect_tables(self.project_name, self.anomaly_table)

        if not re_init:
            self._functions_used_in_project = {}
        else:
            pass  # TODO:

    def change_config_dir(self, config_dir):
        self._initialize(self.project_name, config_dir, True)

    def add_column(
        self, column: Column, name: str = None, column_stage_collection_name: str = None
    ) -> None:
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

    def add_table(self, table: Table) -> None:
        for column in table.columns:
            self.add_column(column)

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
            con_inst = column(name)
            if con_inst["split_on"] is not None:
                name = f'{name}||{con_inst["split_on"][1]}'
            self.columns[name] = column(name)

    def add_external_columns(self, columns):
        self.columns = {**self.columns, **columns}

    def get_project_table(self) -> pd.DataFrame:
        return self.sql_helper.get_project_table()

    def get_anomaly_table(self) -> pd.DataFrame:
        return self.sql_helper.get_anomaly_table()

    def delete_data(self, recreate=True):
        self.sql_helper.delete_data(
            name=self.project_name,
            anomaly_name=self.anomaly_table,
            recreate=recreate,
        )

    def delete_existing_batch(self, trans, batch_name):
        self.sql_helper.delete_existing_batch(trans, batch_name)

    def delete_from_project_config(self):
        self.projects.pop(self.project_name, None)

    def write_functions_to_config(self):
        if "DISPLAY_NAMES" not in self.config[self.project_name]:
            self.config[self.project_name]["DISPLAY_NAMES"] = {}
        self.config[self.project_name]["DISPLAY_NAMES"][
            "DEFAULT"
        ] = self._functions_used_in_project

    def serialize_project(self, use_dill: bool = True) -> None:
        to_dict_cols = copy.deepcopy(self.columns)
        serialized_dict = {}
        for col_name, schema in to_dict_cols.items():
            new_schema = schema
            for idx, function in enumerate(schema["functions"]):
                fun_name = function[0]
                function_obj = function[1]
                fun = codecs.encode(dill.dumps(function_obj), "base64").decode()
                new_schema["functions"][idx] = (function_obj.__module__, fun)
            for idx, function in enumerate(schema["extra_functions"]):
                fun_name = function[0]
                function_obj = function[1]
                fun = codecs.encode(dill.dumps(function_obj), "base64").decode()
                new_schema["extra_functions"][idx] = (function_obj.__module__, fun)
            new_schema["type"] = str(new_schema["type"])
            serialized_dict[col_name] = new_schema
        self.projects[self.project_name] = serialized_dict
        with open(os.path.join(self.config_dir, "projects.json"), "w") as f:
            json.dump(self.projects, f)

    def _load_from_dict(self, column_dict: dict):
        self.columns = column_dict

    def update_config_and_project_files(self):
        self.config.dump()
        with open(os.path.join(self.config_dir, "projects.json"), "w") as f:
            json.dump(self.projects, f)


def load_project(
    config_dir: Union[str, QualipyConfig],
    project_name: str,
    backend: str = "sql",
    reload_functions: bool = None,
) -> Project:
    if isinstance(config_dir, str):
        config_dir = os.path.expanduser(config_dir)
        config = QualipyConfig(config_dir=config_dir, project_name=project_name)
    elif isinstance(config_dir, QualipyConfig):
        config = config_dir
    projects = config.get_projects()
    if project_name not in projects:
        raise Exception(f"{project_name} has not yet been created or serialized")

    project = projects[project_name]
    data_types = DATA_TYPES[backend]

    reconstructed_dict = {}
    for col_name, schema in project.items():
        new_schema = schema
        if not reload_functions:
            for idx, function in enumerate(schema["functions"]):
                fun_name = function[0]
                function_obj = function[1]
                fun = dill.loads(codecs.decode(function_obj.encode(), "base64"))
                new_schema["functions"][idx] = (fun_name, fun)
            for idx, function in enumerate(schema["extra_functions"]):
                fun_name = function[0]
                function_obj = function[1]
                fun = dill.loads(codecs.decode(function_obj.encode(), "base64"))
                new_schema["extra_functions"][idx] = (fun_name, fun)
        else:
            for idx, function in enumerate(schema["functions"]):
                fun_name = function[0]
                function_obj = function[1]
                function_obj = copy_function_spec(fun_name)
                new_schema["functions"][idx] = (
                    function_obj.__name__,
                    function_obj,
                )
            for idx, function in enumerate(schema["extra_functions"]):
                fun_name = function[0]
                function_obj = function[1]
                function_obj = copy_function_spec(fun_name)
                new_schema["extra_functions"][idx] = (
                    function_obj.__name__,
                    function_obj,
                )

        if backend == "pandas":
            new_schema["type"] = data_types[new_schema["type"]]()
        reconstructed_dict[col_name] = new_schema
    project = Project(project_name=project_name, config_dir=config.config_dir)
    project._load_from_dict(reconstructed_dict)
    return project
