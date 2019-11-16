import click
from sqlalchemy import create_engine

from qualipy.web.deploy import FlaskDeploy
from qualipy.anomaly_detection import RunModels

import os
import json


HOME = os.path.expanduser("~")


@click.group()
def qualipy():
    pass


@qualipy.command()
@click.option("--port", default=5005)
@click.option("--host", default="127.0.0.1")
@click.option("--config_dir", default=None)
@click.option(
    "--train_anomaly", default=False, help="Run anomaly models if not preloaded"
)
def run(port, host, config_dir, train_anomaly):
    if config_dir is None:
        config_dir = os.environ["CONFIG_DIR"]
    deployer = FlaskDeploy(
        config_dir=config_dir, host=host, port=port, train_anomaly=train_anomaly
    )
    run_fun = deployer.run()
    run_fun()


@qualipy.command()
@click.option("--project_name", default=False)
@click.option("--config_dir", default=os.path.join(HOME, ".qualipy"))
def train_anomaly(project_name, config_dir):
    with open(os.path.join(config_dir, "config.json"), "r") as file:
        loaded_config = json.load(file)
    try:
        db = loaded_config["db_url"]
    except KeyError("No db specified in config"):
        sys.exit(1)
    engine = create_engine(db)
    run_mods = RunModels(project_name, engine, config_dir)
    run_mods.train_all()


if __name__ == "__main__":
    import sys

    run(sys.argv[1:])
