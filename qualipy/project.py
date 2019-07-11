from qualipy.util import HOME
from qualipy.database import get_table
from qualipy.column import Column

import json
import os
import datetime
import pickle
import pandas as pd
from typing import List, Optional, Union, Dict

from sqlalchemy import engine


class Project(object):
    # TODO: ask michoel about typing when variable can be none
    def __init__(
        self,
        project_name: str,
        backend: str = "pandas",
        engine: engine.base.Engine = None,
        reset: bool = False,
        config_dir: str = None,
    ):
        self.project_name = project_name
        self.backend = backend
        self.columns = {}
        self.reset = reset
        self.config_dir = (
            os.path.join(HOME, ".qualipy") if config_dir is None else config_dir
        )
        if engine is None:
            self.engine = os.path.join(HOME, ".qualipy", "qualipy.db")
        else:
            self.engine = engine

    def add_column(self, column: Column) -> None:
        if isinstance(column, list):
            for col in column:
                self._add_column(col)
        else:
            self._add_column(column)

    def _add_column(self, column: Union[Column, List[Column]]) -> None:
        if isinstance(column.column_name, list):
            for col in column.column_name:
                self.columns[col] = column._as_dict(col)
        else:
            self.columns[column.column_name] = column._as_dict(column.column_name)

    def get_project_table(self) -> pd.DataFrame:
        data = get_table(self.engine, self.project_name)
        data.value = data.value.apply(lambda r: pickle.loads(r))
        return data

    def add_to_project_list(self, schema: Dict[str, str]) -> None:
        project_file_path = os.path.join(self.config_dir, "projects.json")
        try:
            with open(project_file_path, "r") as f:
                projects = json.loads(f.read())
        except:
            projects = {}

        if self.project_name not in projects or self.reset:
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
