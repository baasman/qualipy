import os
import json
import datetime

import click
from qualipy.anomaly.anomaly import _run_anomaly
from qualipy.backends.pandas_backend.generator import BackendPandas
from qualipy.project import generate_config as generate_config_, Project
from qualipy.reports.anomaly import AnomalyReport
from qualipy.reports.comparison import ComparisonReport
from qualipy.reports.batch import BatchReport


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
def generate_config(config_dir_path):
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
    generate_config_(config_dir=config_dir_path)


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
@click.option("--out_file", default=None, type=str, help="Location of the output")
def produce_anomaly_report(
    config_dir,
    project_name,
    run_anomaly,
    clear_anomaly,
    only_show_anomaly,
    t1,
    t2,
    out_file,
):
    """
    CONFIG_DIR: The path to the stored directory

    PROJECT_NAME: Name of the project you want to run an anomaly report on
    """
    view = AnomalyReport(
        config_dir=config_dir,
        project_name=project_name,
        run_anomaly=run_anomaly,
        retrain_anomaly=clear_anomaly,
        only_show_anomaly=only_show_anomaly,
        t1=t1,
        t2=t2,
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
@click.argument("config_dir")
@click.argument("project_name")
@click.option("--recreate", default=True, type=bool)
def clear_data(config_dir, project_name, recreate):
    project = Project(config_dir=config_dir, project_name=project_name, re_init=True)
    print(
        f"Preparing to delete table {project_name} as specified in config dir {config_dir}"
    )
    if click.confirm(
        "Do you wish to continue? Warning - this will permanently delete data"
    ):
        project.delete_data(recreate=recreate)
        project.delete_from_project_config()


if __name__ == "__main__":
    # I do the following to debug cli commands, ignore

    import sys

    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

    produce_batch_report(sys.argv[1:])  # pylint: disable=no-value-for-parameter
