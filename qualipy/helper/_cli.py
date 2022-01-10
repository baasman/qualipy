from qualipy.helper.auto_qpy import (
    setup_auto_qpy_sql_table,
    setup_auto_qpy_pandas_table,
)
import qualipy as qpy

import sqlalchemy as sa


def _setup_sql_table_project(conf, config_dir, project_name, spec) -> qpy.Project:
    table_name = spec["table_name"]
    schema = spec.get("schema")
    tracking_db = spec["db"]
    if tracking_db is not None:
        url = sa.engine.URL.create(**conf["TRACKING_DBS"][tracking_db])
        engine = sa.create_engine(url)
    else:
        engine = None
    columns = spec.get("columns", "all")
    functions = spec.get("functions")
    extra_functions = spec.get("extra_functions")
    types = spec.get("types")
    ignore = spec.get("ignore")
    int_as_cat = spec.get("int_as_cat")
    project = setup_auto_qpy_sql_table(
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
    return project


def _setup_pandas_table_project(
    sample_data, conf, config_dir, project_name, spec
) -> qpy.Project:
    columns = spec.get("columns", "all")
    functions = spec.get("functions")
    extra_functions = spec.get("extra_functions")
    types = spec.get("types")
    ignore = spec.get("ignore")
    int_as_cat = spec.get("int_as_cat")
    overwrite_type = spec.get("overwrite_type")
    as_cat = spec.get("as_cat")
    split_on = spec.get("split_on")
    table = setup_auto_qpy_pandas_table(
        sample_data=sample_data,
        project_name=project_name,
        configuration_dir=config_dir,
        columns=columns,
        functions=functions,
        extra_functions=extra_functions,
        types=types,
        ignore=ignore,
        int_as_cat=int_as_cat,
        overwrite_type=overwrite_type,
        as_cat=as_cat,
        split_on=split_on,
    )
    return table
