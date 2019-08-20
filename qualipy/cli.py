import click
from werkzeug.serving import run_simple

from qualipy_web import app

import os


@click.group()
def qualipy():
    pass


@qualipy.command()
@click.option("--port", default=5005)
@click.option("--debug", default=True)
@click.option("--ip", default="127.0.0.1")
def run(port, debug, ip):
    run_simple(ip, port=port, use_reloader=True, use_debugger=debug)
