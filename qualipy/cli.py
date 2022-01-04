import os
import json
import datetime
from functools import reduce

import click
import sqlalchemy as sa

from qualipy.helper.auto_qpy import auto_qpy_single_batch_sql
from qualipy.anomaly.anomaly import _run_anomaly
from qualipy.backends.pandas_backend.generator import BackendPandas
from qualipy.project import generate_config as generate_config_, Project, load_project
from qualipy.reports.anomaly import AnomalyReport
from qualipy.reports.comparison import ComparisonReport
from qualipy.reports.batch import BatchReport
from qualipy.helper._cli import _setup_pandas_table_project, _setup_sql_table_project
import qualipy as qpy


# DEPLOYMENT_OPTIONS = {"flask": FlaskDeploy, "gunicorn": GUnicornDeploy}

BACKEND = BackendPandas


@click.group()
def qualipy():
    """
    The main entrypoint for interacting with Qualipy.

    Note: Nearly all these functions rely on the configuration specification

    """
    pass


@qualipy.command()
@click.argument("config_dir_path")
@click.option("--keyword_arg", "--kwarg", multiple=True)
def generate_config(config_dir_path, keyword_arg):
    """
    Arguments:
        config_dir_path: The path to the stored directory

    This will generate a folder with the following subfolders and files:
        * config.json - The most important file. This controls everything - from how to generate
            the reports, what anomaly model to use, where the data is stored, etc... See (link here)
            for documentation on the config
        * models - This folder will store binary versions of your anomaly models used for all projects
            specified in the config
    """
    overwrite_kwargs = [i.split(":::") for i in keyword_arg]
    overwrite_kwargs = {k[0]: k[1] for k in overwrite_kwargs}
    generate_config_(config_dir=config_dir_path, overwrite_kwargs=overwrite_kwargs)


@qualipy.command()
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


def produce_anomaly_report_cli(
    config_dir,
    project_name,
    run_anomaly=False,
    clear_anomaly=False,
    only_show_anomaly=False,
    t1=None,
    t2=None,
    out_file=None,
    run_name=None,
):
    config_dir = os.path.expanduser(config_dir)
    view = AnomalyReport(
        config_dir=config_dir,
        project_name=project_name,
        run_anomaly=run_anomaly,
        retrain_anomaly=clear_anomaly,
        only_show_anomaly=only_show_anomaly,
        t1=t1,
        t2=t2,
        run_name=run_name,
    )
    rendered_page = view.render(
        template=f"anomaly.j2", title="Anomaly Report", project_name=project_name
    )
    if out_file is None:
        time_of_run = datetime.datetime.now().strftime("%Y-%d-%mT%H")
        out_file = os.path.join(
            config_dir, "reports", "anomaly", f"{project_name}-{time_of_run}.html"
        )
    rendered_page.dump(out_file)


@qualipy.command()
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


@qualipy.command()
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


def produce_batch_report_cli(
    config_dir, project_name, batch_name, run_name=None, out_file=None
):
    config_dir = os.path.expanduser(config_dir)
    if run_name is None:
        run_name = "0"

    view = BatchReport(
        config_dir=config_dir,
        project_name=project_name,
        batch_name=batch_name,
        run_name=run_name,
    )
    rendered_page = view.render(
        template=f"batch.j2",
        title="Batch Report",
        project_name=project_name,
    )
    if out_file is None:
        time_of_run = datetime.datetime.now().strftime("%Y-%d-%mT%H")
        out_file = os.path.join(
            config_dir,
            "reports",
            "profiler",
            f"{project_name}-{batch_name}-{time_of_run}.html",
        )
    rendered_page.dump(out_file)


@qualipy.command()
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


@qualipy.command()
@click.argument("config_dir")
@click.argument("project_name")
@click.option("--recreate", default=True, type=bool)
@click.option("--confirm", default=True, type=bool)
def clear_data(config_dir, project_name, recreate, confirm):
    clear_data_cli(config_dir, project_name, recreate, confirm)


@qualipy.command()
@click.argument("config_dir")
@click.argument("project_name")
def setup_project(config_dir, project_name):
    with open(os.path.join(config_dir, "config.json"), "r") as f:
        conf = json.load(f)

    if "PROJECT_SPEC" not in conf:
        raise Exception(
            f"Must specify PROJECT_SPEC in config to use this functionality"
        )

    if project_name not in conf["PROJECT_SPEC"]:
        raise Exception(f"Must specify {project_name} in PROJECT_SPEC")

    spec = conf["PROJECT_SPEC"][project_name]
    project = _setup_sql_table_project(
        conf=conf, config_dir=config_dir, project_name=project_name, spec=spec
    )
    project.serialize_project()


@qualipy.command()
@click.argument("config_dir")
@click.argument("project_name")
@click.option("--table_name", multiple=True, required=True)
@click.option("--tracking_db", required=True)
@click.option("--run_anomaly", default=False)
@click.option("--produce_report", default=False)
@click.option("--run_name", default=None)
def run_sql_batch(
    config_dir,
    project_name,
    table_name,
    tracking_db,
    run_anomaly,
    produce_report,
    run_name,
):
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
            produce_report=produce_report,
            engine=tracking_engine,
            commit=False,
        )
    batch.commit()


if __name__ == "__main__":
    # I do the following to debug cli commands, ignore

    import sys

    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

    setup_project(sys.argv[1:])  # pylint: disable=no-value-for-parameter
