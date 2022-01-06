import click

from qualipy.cli.qualipy.commands import (
    clear_data,
    run_pandas_batch,
    run_sql_batch,
    generate_config,
    add_tracking_db,
    setup_pandas_project,
    setup_sql_project,
)


@click.group()
def qualipy():
    """
    The main entrypoint for interacting with Qualipy.
    """
    pass


qualipy.add_command(clear_data)
qualipy.add_command(run_sql_batch)
qualipy.add_command(run_pandas_batch)
qualipy.add_command(generate_config)
qualipy.add_command(add_tracking_db)
qualipy.add_command(setup_pandas_project)
qualipy.add_command(setup_sql_project)