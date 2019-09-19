import click
from werkzeug.serving import run_simple
from sqlalchemy import create_engine

from qualipy_web.app import app
from qualipy.anomaly_detection import RunModels

import os
import json
import sys


HOME = os.path.expanduser("~")


@click.group()
def qualipy():
    pass


@qualipy.command()
@click.option("--port", default=5005)
@click.option("--debug", default=False)
@click.option("--ip", default="localhost")
@click.option(
    "--db",
    default=None,
    help="The standard connection string used by SqlAlchemy. "
    "Has to be identical to where the data is stored",
)
@click.option("--config", default=os.path.join(HOME, ".qualipy", "config.json"))
def run(port, debug, ip, db, config):
    if db is not None:
        with open(config, "r") as file:
            loaded_config = json.load(file)
        loaded_config["db_url"] = db
        with open(config, "w") as file:
            json.dump(loaded_config, file)
        os.environ["QUALIPY_CONFIG_FILE"] = config

    run_simple(ip, port, app, use_reloader=False, use_debugger=debug)


@qualipy.command()
@click.option("--project_name", default=False)
@click.option(
    "--db",
    default=None,
    help="The standard connection string used by SqlAlchemy. "
    "Has to be identical to where the data is stored",
)
@click.option("--config_dir", default=os.path.join(HOME, ".qualipy"))
def train_anomaly(project_name, db, config_dir):
    if db is None:
        with open(os.path.join(config_dir, "config.json"), "r") as file:
            loaded_config = json.load(file)
        try:
            db = loaded_config["db_url"]
        except KeyError("No db given or specified in config"):
            sys.exit(1)
    engine = create_engine(db)
    run_mods = RunModels(project_name, engine, config_dir)
    run_mods.train_all()
