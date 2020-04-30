import click
from sqlalchemy import create_engine
from qualipy.anomaly_detection import _run_anomaly
from qualipy.web.deploy import FlaskDeploy, GUnicornDeploy
from qualipy.web._config import _Config
from qualipy.anomaly_detection import anomaly_data_all_projects
from qualipy._sql import SQLite
from qualipy.backends.pandas_backend.generator import BackendPandas

import os
import json


HOME = os.path.expanduser("~")

DEPLOYMENT_OPTIONS = {"flask": FlaskDeploy, "gunicorn": GUnicornDeploy}

backend = BackendPandas


@click.group()
def qualipy():
    pass


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


if __name__ == "__main__":
    import sys

    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

    run_anomaly(sys.argv[1:])
