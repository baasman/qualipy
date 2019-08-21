import click
from werkzeug.serving import run_simple

from qualipy_web.app import app

import os
import json


HOME = os.path.expanduser("~")


@click.group()
def qualipy():
    pass


@qualipy.command()
@click.option("--port", default=5005)
@click.option("--debug", default=True)
@click.option("--ip", default="localhost")
@click.option("--db", default=None)
@click.option("--config", default=None)
def run(port, debug, ip, db, config):
    config_file = config if config else os.path.join(HOME, ".qualipy", "config.json")
    if db is not None:
        with open(config_file, "r") as file:
            config = json.load(file)
        config["db_url"] = db
        with open(config_file, "w") as file:
            json.dump(config, file)

    run_simple(ip, port, app, use_reloader=True, use_debugger=debug)
