import click
from sqlalchemy import create_engine
from qualipy.anomaly_detection import RunModels
from qualipy.web.deploy import FlaskDeploy, GUnicornDeploy
from qualipy.web._config import _Config

import os
import json


HOME = os.path.expanduser("~")

DEPLOYMENT_OPTIONS = {"flask": FlaskDeploy, "gunicorn": GUnicornDeploy}


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
        config_dir = os.environ["CONFIG_DIR"]
    _Config.config_dir = config_dir

    deployer = DEPLOYMENT_OPTIONS[engine](
        config_dir=config_dir, host=host, port=port, train_anomaly=train_anomaly
    )
    deployer.run()


@qualipy.command()
@click.option("--project_name", default=None)
@click.option("--config_dir", default=None)
def train_anomaly(project_name, config_dir):
    with open(os.path.join(config_dir, "config.json"), "r") as file:
        loaded_config = json.load(file)
    engine = create_engine(loaded_config["QUALIPY_DB"])
    run_mods = RunModels(project_name, engine, config_dir)
    run_mods.train_all()


if __name__ == "__main__":
    import sys

    run(sys.argv[1:])
