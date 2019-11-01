import click
from werkzeug.serving import run_simple
from sqlalchemy import create_engine

from qualipy_web.app import app
from qualipy.anomaly_detection import RunModels

import os
import json


HOME = os.path.expanduser("~")


@click.group()
def qualipy():
    pass


@qualipy.command()
@click.option("--port", default=5005)
@click.option("--debug", default=False)
@click.option("--ip", default="localhost")
@click.option(
    "--config_dir",
    default=os.path.join(HOME, ".qualipy"),
    help="The path of the config file located in your respective .qualipy folder",
)
@click.option(
    "--with_anomaly", default=False, help="Run anomaly models if not preloaded"
)
def run(port, debug, ip, config_dir, with_anomaly):
    config_file = os.path.join(config_dir, "config.json")
    project_file = os.path.join(config_dir, "projects.json")
    os.environ["QUALIPY_CONFIG_FILE"] = config_file
    os.environ["QUALIPY_PROJECT_FILE"] = project_file
    run_simple(ip, port, app, use_reloader=False, use_debugger=debug)


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
