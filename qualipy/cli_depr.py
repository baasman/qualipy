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
from qualipy.project import Project, load_project
from qualipy.reports.anomaly import AnomalyReport
from qualipy.reports.comparison import ComparisonReport
from qualipy.reports.batch import BatchReport
from qualipy.helper._cli import _setup_pandas_table_project, _setup_sql_table_project
import qualipy as qpy
from reflect.column import column


@click.group()
def qualipy_depr():
    """
    The main entrypoint for interacting with Qualipy.

    Note: Nearly all these functions rely on the configuration specification

    """
    pass


@qualipy_depr.command()
@click.option(
    "--project_name",
    default=None,
    help="Name of the project. Must correspond to an existing project",
)
@click.option(
    "--config_dir", default=None, help="Name of an existing configuration directory"
)
@click.option(
    "--retrain",
    default=False,
    type=bool,
    help="If set to true, will clear all models and stored anomaly data, and retrain each model",
)
def run_anomaly(project_name, config_dir, retrain):
    """
    Runs the anomaly models for a specific project, based on the config
    """
    _run_anomaly(project_name, config_dir, retrain)


@qualipy_depr.command()
@click.argument("config_dir", default=None)
@click.argument("project_name", default=None, type=str)
@click.option(
    "--run_anomaly",
    default=False,
    type=bool,
    help="If set to True, qualipy will first run each anomaly model on the data",
)
@click.option(
    "--clear_anomaly",
    default=False,
    type=bool,
    help="If set to True, qualipy will clear all stored anomalies and retrain each model",
)
@click.option(
    "--only_show_anomaly",
    default=False,
    type=bool,
    help="If set to True, only trends containing an anomaly will be shown in the report",
)
@click.option(
    "--t1",
    default=None,
    type=str,
    help="If set, all visualizations in the report will be starting on this date",
)
@click.option(
    "--t2",
    default=None,
    type=str,
    help="If set, all visualizations in the report will be prior to this date",
)
@click.option(
    "--run_name",
    default=None,
    type=str,
    help="",
)
@click.option("--out_file", default=None, type=str, help="Location of the output")
def produce_anomaly_report(
    config_dir,
    project_name,
    run_anomaly,
    clear_anomaly,
    only_show_anomaly,
    t1,
    t2,
    run_name,
    out_file,
):
    """
    CONFIG_DIR: The path to the stored directory

    PROJECT_NAME: Name of the project you want to run an anomaly report on
    """
    produce_anomaly_report_cli(
        config_dir,
        project_name,
        run_anomaly,
        clear_anomaly,
        only_show_anomaly,
        t1,
        t2,
        out_file,
    )


@qualipy_depr.command()
@click.argument("config_dir", default=None)
@click.argument("comparison_name", default=None, type=str)
@click.option("--out_file", default=None, type=str, help="The name of the output file")
def produce_comparison_report(
    config_dir,
    comparison_name,
    out_file,
):
    """
    CONFIG_DIR: The path to the stored directory

    COMPARISON_NAME: Name of the as specified in the configuration file
    """
    view = ComparisonReport(
        config_dir=config_dir,
        comparison_name=comparison_name,
    )
    rendered_page = view.render(
        template=f"comparison.j2",
        title="Comparison Report",
        project_name=comparison_name,
    )

    if out_file is None:
        time_of_run = datetime.datetime.now().strftime("%Y-%d-%mT%H")
        out_file = os.path.join(
            config_dir,
            "reports",
            "comparison",
            f"{comparison_name}-{time_of_run}.html",
        )

    rendered_page.dump(out_file)


@qualipy_depr.command()
@click.argument("config_dir", default=None)
@click.argument("project_name", default=None, type=str)
@click.argument("batch_name", default=None, type=str)
@click.option("--run_name", default=None, type=str)
@click.option("--out_file", default=None, type=str, help="The name of the output file")
def produce_batch_report(
    config_dir,
    project_name,
    batch_name,
    run_name,
    out_file,
):
    """
    CONFIG_DIR: The path to the stored directory

    PROJECT_NAME: Name of the batch you want to report on. Must be present in profile_data

    BATCH_NAME: Name of the batch you want to report on. Must be present in profile_data
    """
    produce_batch_report_cli(config_dir, project_name, batch_name, run_name, out_file)


if __name__ == "__main__":
    # I do the following to debug cli commands, ignore

    import sys
    from qualipy.cli.qualipy.commands import (
        run_pandas_batch,
        run_sql_batch,
        setup_sql_project,
        generate_config,
    )

    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

    setup_sql_project(sys.argv[1:])  # pylint: disable=no-value-for-parameter
