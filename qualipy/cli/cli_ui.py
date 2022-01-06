from qualipy.cli import cli

from click_web import create_click_web_app

app = create_click_web_app(cli, cli.qualipy)