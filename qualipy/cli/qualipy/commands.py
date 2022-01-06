import os
import json
import datetime
from functools import reduce
from collections import defaultdict

import click
import sqlalchemy as sa
import pandas as pd

from qualipy.helper.auto_qpy import (
    auto_qpy_single_batch_pandas,
    auto_qpy_single_batch_sql,
)
from qualipy.anomaly.anomaly import _run_anomaly
from qualipy.backends.pandas_backend.generator import BackendPandas
from qualipy.project import generate_config as generate_config_, Project, load_project
from qualipy.reports.anomaly import AnomalyReport
from qualipy.reports.comparison import ComparisonReport
from qualipy.reports.batch import BatchReport
from qualipy.helper._cli import _setup_pandas_table_project, _setup_sql_table_project
import qualipy as qpy


@click.command()
@click.argument("config_dir")
@click.option("--keyword_arg", "--kwarg", multiple=True)
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
    overwrite_kwargs = [i.split(":::") for i in keyword_arg]
    overwrite_kwargs = {k[0]: k[1] for k in overwrite_kwargs}
    generate_config_(config_dir=config_dir, overwrite_kwargs=overwrite_kwargs)


@click.command()
@click.argument("config_dir")
@click.argument("name")
@click.option("--drivername", required=True)
@click.option("--username", required=True)
@click.option("--password", required=True)
@click.option("--host", required=True)
@click.option("--port", required=True)
@click.option("--query", required=False, nargs=2, type=(str, str), default=None)
def add_tracking_db(
    config_dir, name, drivername, username, password, host, port, query
):

    with open(os.path.join(config_dir, "config.json"), "r") as f:
        conf = json.load(f)
    if "TRACKING_DBS" not in conf:
        conf["TRACKING_DBS"] = {}

    spec = dict(
        drivername=drivername,
        username=username,
        password=password,
        host=host,
        port=port,
    )
    if query is not None:
        query_args = {}
        for inp in query:
            query_args[inp[0]] = inp[1]
        spec["query"] = query_args
    conf["TRACKING_DBS"][name] = spec
    with open(os.path.join(config_dir, "config.json"), "w") as f:
        json.dump(conf, f)


@click.command()
@click.argument("config_dir")
@click.argument("project_name")
@click.option("--tracking-db", type=str, required=True)
@click.option("--table-name", type=str, required=True)
@click.option("--schema", type=str)
@click.option("--int_as_cat", type=bool, default=False, show_default=True)
@click.option("--columns", type=str, help="To specify multiple, use comma as delimiter")
@click.option(
    "--function",
    nargs=2,
    type=(str, str),
    multiple=True,
    help="This expects two values. The first is the ",
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
    with open(os.path.join(config_dir, "config.json"), "r") as f:
        conf = json.load(f)
    if "PROJECT_SPEC" not in conf:
        conf["PROJECT_SPEC"] = {}

    extra_functions = defaultdict(list)
    for inp in function:
        extra_functions[inp[0]].append(inp[1])

    project_spec = {
        "table_type": "sql",
        "db": tracking_db,
        "table_name": table_name,
        "int_as_cat": int_as_cat,
        "schema": schema,
        "columns": columns.split(","),
        "extra_functions": extra_functions,
    }
    conf["PROJECT_SPEC"][project_name] = project_spec
    with open(os.path.join(config_dir, "config.json"), "w") as f:
        json.dump(conf, f)
    project = _setup_sql_table_project(
        conf=conf, config_dir=config_dir, project_name=project_name, spec=project_spec
    )
    project.serialize_project()


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
    with open(os.path.join(config_dir, "config.json"), "r") as f:
        conf = json.load(f)
    if "PROJECT_SPEC" not in conf:
        conf["PROJECT_SPEC"] = {}

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
    conf["PROJECT_SPEC"][project_name] = project_spec
    with open(os.path.join(config_dir, "config.json"), "w") as f:
        json.dump(conf, f)
    project = _setup_pandas_table_project(
        sample_data=sample_data,
        conf=conf,
        config_dir=config_dir,
        project_name=project_name,
        spec=project_spec,
    )
    project.serialize_project()


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
def run_sql_batch(
    config_dir,
    project_name,
    table_name,
    tracking_db,
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
    url = sa.engine.URL.create(**project.config["TRACKING_DBS"][tracking_db])
    tracking_engine = sa.create_engine(url)
    run_name = table_name if run_name is None else run_name
    batch = None
    for table in table_name:
        batch = auto_qpy_single_batch_sql(
            batch=batch,
            table_name=table,
            project=project,
            run_anomaly=run_anomaly,
            run_name=run_name,
            produce_report=False,
            engine=tracking_engine,
            commit=False,
        )
    batch.commit()


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
        qpy.cli.produce_anomaly_report_cli(
            config_dir=project.config_dir,
            project_name=project.project_name,
            run_anomaly=run_anomaly,
            run_name=run_name,
        )


def clear_data_cli(config_dir, project_name, recreate=True, confirm=True):
    project = Project(config_dir=config_dir, project_name=project_name, re_init=True)
    print(
        f"Preparing to delete table {project_name} as specified in config dir {config_dir}"
    )
    if confirm:
        if click.confirm(
            "Do you wish to continue? Warning - this will permanently delete data"
        ):
            project.delete_data(recreate=recreate)
            if not recreate:
                project.delete_from_project_config()
    else:
        project.delete_data(recreate=recreate)
        if not recreate:
            project.delete_from_project_config()


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