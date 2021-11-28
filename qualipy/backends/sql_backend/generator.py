from qualipy.exceptions import InvalidReturnValue
from qualipy.util import get_column
from qualipy.backends.base import BackendBase
from qualipy.exceptions import InvalidType
from qualipy.backends.sql_backend.dataset import SQLData
from qualipy.backends.sql_backend.functions import (
    value_counts,
    percentage_missing,
    is_unique,
)

import warnings
import datetime
from typing import Optional, Union, List, Dict, Any, Callable
import uuid
import pickle
import logging

from numpy import NaN
import pandas as pd
import sqlalchemy as sa


logger = logging.getLogger(__name__)


Column = Dict[str, Union[str, bool, Dict[str, Callable]]]


class BackendSQL(BackendBase):

    def set_schema(
        self, data, columns: Dict[str, Column], current_name: str
    ) -> Dict[str, Dict[str, Union[bool, str]]]:
        schema = {
            f"{info['name']}_{current_name}": {
                "nullable": info["null"],
                "unique": info["unique"],
                "dtype": info["type"],
                "column_name": info["name"],
            }
            for col, info in columns.items()
        }
        return schema

    def return_data_copy(self, data):
        return data

    def get_shape(self, data):
        if data.custom_where is None:
            count_query = sa.select([sa.func.count()]).select_from(data._table)
        else:
            count_query = (
                sa.select([sa.func.count()])
                .select_from(data._table)
                .where(sa.text(data.custom_where))
            )
        rows = int(data.engine.execute(count_query).scalar())
        columns = len(data.table_reflection)
        return rows, columns

    def get_dtype(self, data, column):
        # is there any point retrieving type?
        try:
            col = [i for i in data.table_reflection if i["name"] == column][0]
        except IndexError:
            logger.warn(f"Column: {column} not found in table {data._table.fullname}")
            return "Unknown"
        except AttributeError:
            raise Exception(f"A pandas dataframe was pass to a SQL backend")

        return str(col["type"])

    def check_type(self, data, column, desired_type, force=False):
        # no point here
        pass

    def generate_column_general_info(self, specs, data, time_of_run, run_name):
        col_name = specs["name"]
        if specs["unique"]:
            unique = self.generate_description(
                function=is_unique,
                data=data,
                column=col_name,
                function_name="is_unique",
                date=time_of_run,
                viz_type="data-characteristic",
                kwargs={},
            )
            unique["run_name"] = run_name
        else:
            unique = None
        if specs["is_category"]:
            value_props = self.generate_description(
                function=value_counts,
                data=data,
                column=col_name,
                function_name="value_counts",
                date=time_of_run,
                viz_type="categorical",
                kwargs={},
                return_format="dict",
            )
            value_props = None if str(value_props["value"]) == "nan" else value_props
            if value_props is not None:
                value_props["run_name"] = run_name
        else:
            value_props = None
        perc_missing = self.generate_description(
            function=percentage_missing,
            data=data,
            column=col_name,
            function_name="perc_missing",
            date=time_of_run,
            viz_type="data-characteristic",
            kwargs={},
        )
        perc_missing["run_name"] = run_name
        return unique, perc_missing, value_props

    def get_chunks(self, data, time_freq, time_column):
        pass

    def overwrite_type(self, data, col, type):
        return data

    def generate_data(self, *args, **kwargs):
        data = SQLData(*args, **kwargs)
        return data.get_data()
