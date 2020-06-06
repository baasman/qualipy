import click
from sqlalchemy import create_engine
from qualipy.anomaly_detection import _run_anomaly
from qualipy.web.deploy import FlaskDeploy, GUnicornDeploy
from qualipy.web._config import _Config
from qualipy.anomaly_detection import anomaly_data_all_projects
from qualipy.backends.pandas_backend.generator import BackendPandas
from qualipy.project import create_qualipy_folder, Project
from qualipy.reports.view import AnomalyReport, ComparisonReport

import os
import json


HOME = os.path.expanduser("~")

DEPLOYMENT_OPTIONS = {"flask": FlaskDeploy, "gunicorn": GUnicornDeploy}

backend = BackendPandas


@click.group()
def qualipy():
    pass


@qualipy.command()
@click.argument("config_dir_path")
@click.option("--db_url", default=None)
def generate_config(config_dir_path, db_url):
    create_qualipy_folder(config_dir=config_dir_path, db_url=db_url)


@qualipy.command()
@click.argument("config_dir")
@click.argument("project_name")
@click.option("--source-data", default=True, type=bool)
@click.option("--anomaly-data", default=True, type=bool)
def clear_data(config_dir, project_name, source_data, anomaly_data):
    project = Project(config_dir=config_dir, project_name=project_name)
    project.delete_data(source_data=source_data, anomaly=anomaly_data)
    project.delete_from_project_config()


@qualipy.command()
@click.option("--port", default=5005)
@click.option("--host", default="127.0.0.1")
@click.option("--config_dir", default=None)
@click.option("--engine", default="flask")
@click.option(
    "--train_anomaly",
    default=False,
    type=bool,
    help="Run anomaly models if not preloaded",
)
def run(port, host, config_dir, train_anomaly, engine):
    if config_dir is None:
        config_dir = os.environ["QUALIPY_CONFIG_DIR"]
    _Config.config_dir = config_dir
    _Config.train_anomaly = train_anomaly

    host = os.getenv("QUALIPY_HOST", host)
    port = os.getenv("QUALIPY_PORT", port)
    engine = os.getenv("QUALIPY_ENGINE", engine)
    train_anomaly = os.getenv("QUALIPY_TRAIN_ANOMALY", train_anomaly)

    deployer = DEPLOYMENT_OPTIONS[engine](
        config_dir=config_dir, host=host, port=port, train_anomaly=train_anomaly
    )
    deployer.run()


@qualipy.command()
@click.option("--project_name", default=None)
@click.option("--config_dir", default=None)
@click.option("--retrain", default=False, type=bool)
def run_anomaly(project_name, config_dir, retrain):
    _run_anomaly(backend, project_name, config_dir, retrain)


@qualipy.command()
@click.option("--config_dir", default=None)
@click.option("--retrain", default=False, type=bool)
def schedule_anomaly(config_dir, retrain):
    with open(os.path.join(config_dir, "config.json"), "rb") as cf:
        config = json.load(cf)

    scheduler_conf = config["ANOMALY_SCHEDULER"]
    project_names = scheduler_conf["PROJECTS"]
    for project_name in project_names:
        _run_anomaly(backend, project_name, config_dir, retrain)


@qualipy.command()
@click.option("--config_dir", default=None)
@click.option("--project_name", default=None, type=str)
@click.option("--run_anomaly", default=False, type=bool)
@click.option("--clear_anomaly", default=False, type=bool)
@click.option("--only_show_anomaly", default=False, type=bool)
@click.option("--t1", default=None, type=str)
@click.option("--t2", default=None, type=str)
@click.option("--out_file", default=None, type=str)
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
    view = AnomalyReport(
        config_dir=config_dir,
        project_name=project_name,
        run_anomaly=run_anomaly,
        retrain_anomaly=clear_anomaly,
        t1=t1,
        t2=t2,
    )
    rendered_page = view.render(
        template=f"anomaly.j2", title="Anomaly Report", project_name=project_name
    )
    if out_file is None:
        out_file = f"anomaly_report_{project_name}.html"
    rendered_page.dump(out_file)


@qualipy.command()
@click.option("--config_dir", default=None)
@click.option("--comparison_name", default=None, type=str)
@click.option("--t1", default=None, type=str)
@click.option("--t2", default=None, type=str)
@click.option("--out_file", default=None, type=str)
def produce_comparison_report(
    config_dir, comparison_name, t1, t2, out_file,
):
    view = ComparisonReport(
        config_dir=config_dir, comparison_name=comparison_name, t1=t1, t2=t2,
    )
    rendered_page = view.render(
        template=f"comparison.j2",
        title="Comparison Report",
        project_name=comparison_name,
    )
    if out_file is None:
        out_file = f"comparison_report_{comparison_name}.html"
    rendered_page.dump(out_file)


if __name__ == "__main__":
    import sys

    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

    produce_comparison_report(sys.argv[1:])
