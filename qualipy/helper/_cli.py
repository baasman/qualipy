from qualipy.helper.auto_qpy import setup_auto_qpy_sql_table
import qualipy as qpy

import sqlalchemy as sa


def _setup_sql_table_project(conf, config_dir, project_name, spec) -> qpy.Project:
    table_name = spec["table_name"]
    schema = spec.get("schema")
    tracking_db = spec["db"]
    url = sa.engine.URL.create(**conf["TRACKING_DBS"][tracking_db])
    engine = sa.create_engine(url)
    columns = spec.get("columns", "all")
    functions = spec.get("functions")
    extra_functions = spec.get("extra_functions")
    types = spec.get("types")
    ignore = spec.get("ignore")
    int_as_cat = spec.get("int_as_cat")
    table = setup_auto_qpy_sql_table(
        table_name=table_name,
        project_name=project_name,
        schema=schema,
        engine=engine,
        configuration_dir=config_dir,
        columns=columns,
        functions=functions,
        extra_functions=extra_functions,
        types=types,
        ignore=ignore,
        int_as_cat=int_as_cat,
    )
    return table


def _setup_pandas_table_project(spec):
    table_name = spec["table_name"]
