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

from qualipy.util import HOME
from qualipy._sql import DB_ENGINES
from qualipy.reflect.column import Column
from qualipy.reflect.table import Table
from qualipy._schema import config_schema
from qualipy.backends.data_types import PANDAS_TYPES
from qualipy.config import DEFAULT_PROJECT_CONFIG


DATA_TYPES = {"pandas": PANDAS_TYPES, "sql": {}}


def _validate_project_name(project_name):
    assert "-" not in project_name


def set_default_config(config_dir, db_url=None):
    conf = {}
    if db_url is None:
        db_url = f'sqlite:///{os.path.join(config_dir, "qualipy.db")}'
    conf["QUALIPY_DB"] = db_url
    return conf


def _generate_config(config_dir, db_url):
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


def generate_config(config_dir, create_in_empty_dir=False, db_url=None):
    config_dir = os.path.expanduser(config_dir)
    if not os.path.exists(config_dir):
        _generate_config(config_dir=config_dir, db_url=db_url)
    else:
        config_already_exists = os.path.exists(os.path.join(config_dir, "config.json"))
        if create_in_empty_dir and not config_already_exists:
            _generate_config(config_dir=config_dir, db_url=db_url)
        if not config_already_exists:
            raise Exception(
                "Error: Make sure directory follows proper Qualipy structure"
            )


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
        self._write_default_config_if_not_exists()
        with open(os.path.join(self.config_dir, "config.json"), "rb") as f:
            self.config = json.load(f)
        self._validate_schema(self.config)
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
            pass  # read from config

        self.project_file_path = os.path.join(self.config_dir, "projects.json")
        try:
            with open(self.project_file_path, "r") as f:
                projects = json.loads(f.read())
        except:
            projects = {}
        self.projects = projects

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

    def _validate_schema(self, config):
        # config_schema.validate(config)
        pass

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
        return self.sql_helper.get_anomaly_table(self.project_name)

    def delete_data(self, recreate=True):
        self.sql_helper.delete_data(
            name=self.project_name,
            anomaly_name=self.anomaly_table,
            recreate=recreate,
        )

    def delete_from_project_config(self):
        self.projects.pop(self.project_name, None)

    def _write_default_config_if_not_exists(self):
        with open(os.path.join(self.config_dir, "config.json"), "rb") as f:
            config = json.load(f)
        if self.project_name not in config:
            config[self.project_name] = DEFAULT_PROJECT_CONFIG
        with open(os.path.join(self.config_dir, "config.json"), "w") as f:
            json.dump(config, f)

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
                new_schema["functions"][idx] = (fun_name, fun)
            for idx, function in enumerate(schema["extra_functions"]):
                fun_name = function[0]
                function_obj = function[1]
                fun = codecs.encode(dill.dumps(function_obj), "base64").decode()
                new_schema["extra_functions"][idx] = (fun_name, fun)
            new_schema["type"] = str(new_schema["type"])
            serialized_dict[col_name] = new_schema
        self.projects[self.project_name] = serialized_dict
        with open(self.project_file_path, "w") as f:
            json.dump(self.projects, f)

    def _load_from_dict(self, column_dict: dict):
        self.columns = column_dict

    def update_config_and_project_files(self):
        with open(os.path.join(self.config_dir, "config.json"), "w") as f:
            json.dump(self.config, f)
        with open(self.project_file_path, "w") as f:
            json.dump(self.projects, f)

    def add_to_project_list(
        self, schema: Dict[str, Dict[str, Union[str, bool]]], reset_config: bool = False
    ) -> None:

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


def load_project(
    config_dir: str, project_name: str, backend: str = "pandas"
) -> Project:
    config_dir = os.path.expanduser(config_dir)
    project_file_path = os.path.join(config_dir, "projects.json")
    with open(project_file_path, "r") as f:
        projects = json.loads(f.read())
    if project_name not in projects:
        raise Exception(f"{project_name} has not yet been created or serialized")

    project = projects[project_name]
    data_types = DATA_TYPES[backend]

    reconstructed_dict = {}
    for col_name, schema in project.items():
        new_schema = schema
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
        if backend == "pandas":
            new_schema["type"] = data_types[new_schema["type"]]()
        reconstructed_dict[col_name] = new_schema
    project = Project(project_name=project_name, config_dir=config_dir)
    project._load_from_dict(reconstructed_dict)
    return project
