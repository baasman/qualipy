from lib2to3.pgen2 import driver
import os
import json
from collections import defaultdict
import logging
import datetime

import click
from requests import delete
import sqlalchemy as sa
import pandas as pd

from qualipy.helper.auto_qpy import (
    auto_qpy_single_batch_pandas,
    auto_qpy_single_batch_sql,
)
from qualipy.project import load_project
from qualipy.helper._cli import _setup_pandas_table_project, _setup_sql_table_project
from qualipy.cli.report.util import produce_anomaly_report_cli, produce_batch_report_cli
from qualipy.cli.qualipy.util import clear_data_cli
from qualipy.config import QualipyConfig, generate_config as generate_config_


logger = logging.getLogger()


@click.command()
@click.argument("config_dir")
@click.option(
    "--keyword_arg", "--kwarg", multiple=True, nargs=2, default=None, type=(str, str)
)
def generate_config(config_dir, keyword_arg):
    """
    Arguments:
        config_dir: The path to the stored directory

    This will generate a folder with the following subfolders and files:
        * config.json - The most important file. This controls everything - from how to generate
            the reports, what anomaly model to use, where the data is stored, etc... See (link here)
            for documentation on the config
        * models - This folder will store binary versions of your anomaly models used for all projects
            specified in the config
    """
    if keyword_arg is not None:
        overwrite_kwargs = {}
        for inp in keyword_arg:
            overwrite_kwargs[inp[0]] = inp[1]
    else:
        overwrite_kwargs = None
    generate_config_(config_dir=config_dir, overwrite_kwargs=overwrite_kwargs)


@click.command()
@click.argument("config_dir")
@click.argument("name")
@click.option("--drivername", required=True)
@click.option("--username", required=True)
@click.option("--password", required=True)
@click.option("--host", required=True)
@click.option("--port", required=True, type=int)
@click.option(
    "--query", required=False, multiple=True, nargs=2, type=(str, str), default=None
)
def add_tracking_db(
    config_dir, name, drivername, username, password, host, port, query
):

    config = QualipyConfig(config_dir=config_dir)
    config.add_tracking_db(
        name=name,
        drivername=drivername,
        username=username,
        password=password,
        host=host,
        port=port,
        query=query,
    )


@click.command()
@click.argument("config_dir")
@click.option("--jdbc-url", required=True)
@click.option("--username", required=True)
@click.option("--password", required=True)
def add_spark_conn(config_dir, jdbc_url, username, password):
    config = QualipyConfig(config_dir=config_dir)
    spec = dict(
        jdbc_url=jdbc_url,
        username=username,
        password=password,
    )
    config["SPARK_CONN"] = spec
    config.dump()


def _setup_sql_project(
    config_dir,
    project_name,
    tracking_db=None,
    table_name=None,
    schema=None,
    int_as_cat=None,
    columns=None,
    function=None,
):
    config = QualipyConfig(config_dir=config_dir)
    if "PROJECT_SPEC" not in config:
        config["PROJECT_SPEC"] = {}

    extra_functions = defaultdict(list)
    if function is not None:
        for inp in function:
            extra_functions[inp[0]].append(inp[1])

    if columns != "all" and columns is not None:
        columns = columns.split(",")

    project_spec = {
        "table_type": "sql",
        "db": tracking_db,
        "table_name": table_name,
        "int_as_cat": int_as_cat,
        "schema": schema,
        "columns": columns,
        "extra_functions": extra_functions,
    }
    config["PROJECT_SPEC"][project_name] = project_spec
    config.dump()
    project = _setup_sql_table_project(
        conf=config, config_dir=config_dir, project_name=project_name, spec=project_spec
    )
    project.save()
    return project


@click.command()
@click.argument("config_dir")
@click.argument("project_name")
@click.option("--tracking-db", type=str, required=False, default=None)
@click.option("--table-name", type=str, required=False, default=None)
@click.option("--schema", type=str, default=None)
@click.option("--int_as_cat", type=bool, default=False, show_default=True)
@click.option(
    "--columns",
    type=str,
    help="To specify multiple, use comma as delimiter",
    required=False,
)
@click.option(
    "--function",
    nargs=2,
    type=(str, str),
    multiple=True,
    help="This expects two values. The first is the ",
    required=False,
)
def setup_sql_project(
    config_dir,
    project_name,
    tracking_db,
    table_name,
    schema,
    int_as_cat,
    columns,
    function,
):
    _setup_sql_project(
        config_dir=config_dir,
        project_name=project_name,
        tracking_db=tracking_db,
        table_name=table_name,
        schema=schema,
        int_as_cat=int_as_cat,
        columns=columns,
        function=function,
    )


@click.command()
@click.argument("config_dir")
@click.argument("project_name")
@click.option(
    "--file-path",
    type=str,
    required=False,
    help="Will be used to infer schema. Must be given if setting columns to `all`",
)
@click.option("--int_as_cat", type=bool, default=False, show_default=True)
@click.option("--overwrite_type", type=bool, default=False, show_default=True)
@click.option("--as_cat", type=str)
@click.option(
    "--columns",
    type=str,
    default="all",
    show_default=True,
    help="To specify multiple, use comma as delimiter",
)
@click.option("--ignore", type=str, help="Columns to ignore")
@click.option(
    "--function",
    nargs=2,
    type=(str, str),
    multiple=True,
    help="This expects two values. The first is the ",
)
def setup_pandas_project(
    config_dir,
    project_name,
    file_path,
    int_as_cat,
    overwrite_type,
    as_cat,
    ignore,
    columns,
    function,
):
    config = QualipyConfig(config_dir=config_dir)
    if "PROJECT_SPEC" not in config:
        config["PROJECT_SPEC"] = {}

    extra_functions = defaultdict(list)
    for inp in function:
        extra_functions[inp[0]].append(inp[1])

    sample_data = pd.read_csv(file_path, nrows=1)

    project_spec = {
        "table_type": "pandas",
        "file_path": file_path,
        "int_as_cat": int_as_cat,
        "overwrite_type": overwrite_type,
        "as_cat": as_cat,
        "columns": columns.split(",") if isinstance(columns, list) else columns,
        "ignore": ignore,
        "extra_functions": extra_functions,
    }
    config["PROJECT_SPEC"][project_name] = project_spec
    config.dump()
    project = _setup_pandas_table_project(
        sample_data=sample_data,
        conf=config,
        config_dir=config_dir,
        project_name=project_name,
        spec=project_spec,
    )
    project.serialize_project()
    return project


def _run_sql_batch(
    config_dir,
    project_name,
    table_name,
    tracking_db,
    run_anomaly=False,
    produce_report=False,
    run_name=None,
    use_spark=False,
    overwrite_kwarg=None,
    custom_select_sql=None,
    backend="sql",
    partition_info=None,
    custom_schema=None,
    reload_functions=False,
    batch_name: str = None,
    delete_existing_batch: bool = False,
    ignore_if_existing_batch: bool = False,
    time_of_run: datetime.datetime = None,
):
    if isinstance(project_name, str):
        project = load_project(
            config_dir=config_dir,
            project_name=project_name,
            backend="sql",
            reload_functions=reload_functions,
        )
    else:
        project = project_name

    if not isinstance(table_name, list):
        table_name = [table_name]
    url = sa.engine.URL.create(**project.config["TRACKING_DBS"][tracking_db])
    tracking_engine = sa.create_engine(url)
    batch = None
    if overwrite_kwarg is not None:
        if isinstance(overwrite_kwarg, tuple):
            overwrite_kwarg = {i[0]: i[1] for i in overwrite_kwarg}
    initial_run_name = run_name
    for table in table_name:
        run_name = table if initial_run_name is None else initial_run_name
        logger.info(f"Table: {table}, Run name: {run_name}")
        batch = auto_qpy_single_batch_sql(
            batch=batch,
            table_name=table,
            project=project,
            run_anomaly=run_anomaly,
            run_name=run_name,
            produce_report=False,
            engine=tracking_engine,
            overwrite_arguments=overwrite_kwarg,
            commit=False,
            use_spark=use_spark,
            partition_info=partition_info,
            custom_schema=custom_schema,
            custom_select_sql=custom_select_sql,
            backend=backend,
            batch_name=batch_name,
            ignore_if_batch_exists=ignore_if_existing_batch,
            time_of_run=time_of_run,
        )
    if len(batch.total_measures) > 0:
        batch.commit(delete_existing_batch=delete_existing_batch)
    if produce_report:
        produce_anomaly_report_cli(
            config_dir=project.config_dir,
            project_name=project.project_name,
            run_anomaly=run_anomaly,
            run_name=run_name,
        )
    return batch.get_total_measures_as_df()


@click.command()
@click.argument("config_dir")
@click.argument("project_name")
@click.option(
    "--table-name",
    multiple=True,
    required=True,
    help="The name of the table that contains the columns specified in the project",
)
@click.option(
    "--tracking-db",
    required=True,
    help="The name of the connection to use to connect to the table, as specified in the config",
)
@click.option("--run-anomaly", default=False, help="Should the anomaly models run?")
@click.option("--produce-report", default=False, help="Should the report be produced")
@click.option("--run-name", default=None, help="Name to associate with the batch run")
@click.option("--use-spark/--sql-only", default=False, help="Use spark when possible")
@click.option(
    "--overwrite-kwarg",
    required=False,
    multiple=True,
    nargs=2,
    type=(str, str),
    default=None,
)
@click.option("--custom-select-sql", default=None, help="When creating a custom table")
@click.option(
    "--backend", default="sql", help="Will qpy functions expect pandas/spark/sql data"
)
@click.option(
    "--reload-functions",
    default=False,
    required=False,
    help="Do you want to reload the functions. Sometimes relevant",
)
@click.option(
    "--batch-name",
    default=None,
    required=False,
    help="Do you want to reload the functions. Sometimes relevant",
)
@click.option(
    "--delete-existing-batch",
    default=False,
    required=False,
    help="If batch of this name already exists, should it be deleted?",
)
def run_sql_batch(
    config_dir,
    project_name,
    table_name,
    tracking_db,
    run_anomaly,
    produce_report,
    run_name,
    use_spark,
    overwrite_kwarg,
    custom_select_sql,
    backend,
    reload_functions,
    batch_name,
    delete_existing_batch,
):
    """
    Arguments:
        config_dir: The path to the configuration directory
        project_name: Existing project for which you want to run a batch
    """
    _run_sql_batch(
        config_dir=config_dir,
        project_name=project_name,
        table_name=table_name,
        tracking_db=tracking_db,
        run_anomaly=run_anomaly,
        produce_report=produce_report,
        run_name=run_name,
        use_spark=use_spark,
        overwrite_kwarg=overwrite_kwarg,
        custom_select_sql=custom_select_sql,
        backend=backend,
        reload_functions=reload_functions,
        batch_name=batch_name,
        delete_existing_batch=delete_existing_batch,
    )


@click.command()
@click.argument("config_dir")
@click.argument("project_name")
@click.option(
    "--file-path",
    multiple=True,
    required=True,
    help="The name of the table that contains the columns specified in the project",
)
@click.option("--run-anomaly", default=False, help="Should the anomaly models run?")
@click.option("--produce-report", default=False, help="Should the report be produced")
@click.option("--run-name", default=None, help="Name to associate with the batch run")
def run_pandas_batch(
    config_dir,
    project_name,
    file_path,
    run_anomaly,
    produce_report,
    run_name,
):
    """
    Arguments:
        config_dir: The path to the configuration directory
        project_name: Existing project for which you want to run a batch
    """
    project = load_project(
        config_dir=config_dir, project_name=project_name, backend="sql"
    )
    batch = None
    for file in file_path:
        # TODO: expand this obviously
        data = pd.read_csv(file)
        batch = auto_qpy_single_batch_pandas(
            data=data,
            batch=batch,
            project=project,
            run_anomaly=run_anomaly,
            run_name=run_name,
            produce_report=False,
            commit=False,
        )
    batch.commit()
    if produce_report:
        produce_anomaly_report_cli(
            config_dir=project.config_dir,
            project_name=project.project_name,
            run_anomaly=run_anomaly,
            run_name=run_name,
        )
    return batch.get_total_measures_as_df()


@click.command()
@click.argument("config_dir")
@click.argument("project_name")
@click.option(
    "--recreate", default=True, type=bool, help="Should the tables be reinstantiated"
)
@click.option(
    "--confirm",
    default=True,
    type=bool,
    help="Should Qualipy ask for permission first?",
)
def clear_data(config_dir, project_name, recreate, confirm):
    """
    Arguments:
        config_dir: The path to the configuration directory
        project_name: The name of the project for which you want to clear data
    """
    clear_data_cli(config_dir, project_name, recreate, confirm)
