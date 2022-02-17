import click

from qualipy.cli.qualipy.commands import (
    clear_data,
    run_pandas_batch,
    run_sql_batch,
    generate_config,
    add_tracking_db,
    setup_pandas_project,
    setup_sql_project,
    add_spark_conn,
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
qualipy.add_command(add_spark_conn)
qualipy.add_command(setup_pandas_project)
qualipy.add_command(setup_sql_project)


if __name__ == "__main__":
    # I do the following to debug cli commands, ignore

    import sys
    import os

    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

    run_pandas_batch(sys.argv[1:])  # pylint: disable=no-value-for-parameter