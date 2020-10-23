import datetime

import sqlalchemy as sa
import pandas as pd

import qualipy as qpy
from qualipy.reflect.column import column
from qualipy.backends.pandas_backend.dataset import PandasData
from qualipy.backends.pandas_backend.pandas_types import (
    ObjectType,
    FloatType,
    DateTimeType,
)


def query_data(query):
    ### SET THIS ###
    conf = {
        "drivername": "",
        "username": "",
        "password": "",
        "host": "",
        "port": "",
        "query": dict(service_name=""),
    }
    url = sa.engine.url.URL(**conf)
    engine = sa.create_engine(url)
    df = pd.read_sql(query, con=engine)
    return df


### SET THIS ###
num_cols = []
cat_cols = []
date_cols = []


def define_project(config_dir, project_name):
    num_columns = []
    for col in num_cols:
        num_columns.append(column(column_name=col, column_type=FloatType()))

    cat_columns = []
    for col in cat_cols:
        cat_columns.append(
            column(column_name=col, column_type=ObjectType(), is_category=True)
        )
    date_columns = []
    for col in date_cols:
        date_columns.append(
            column(column_name=col, column_type=DateTimeType(), is_date=True)
        )
    all_columns = num_columns + cat_columns + date_columns

    project = qpy.Project(project_name=project_name, config_dir=config_dir)
    for col in all_columns:
        project.add_column(col)
    return project


def run_profiler(data, project, batch_name=None, run_name=None):
    batch = qpy.Qualipy(
        project=project,
        backend="pandas",
        time_of_run=datetime.datetime.now(),
        batch_name=batch_name,
    )

    finding_data = PandasData(data)
    batch.set_dataset(finding_data, run_name=run_name)
    batch.run(autocommit=False, profile_batch=True)
    batch.commit()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("config_dir", type=str)
    parser.add_argument("project_name", type=str)
    parser.add_argument("query", type=str)
    parser.add_argument("--batch_name", type=str)
    parser.add_argument("--run_name", type=str)

    args = parser.parse_args()

    batch_name = (
        args.batch_name if args.batch_name is not None else f"{args.project_name}_full"
    )
    run_name = args.run_name if args.run_name is not None else f"{args.project_name}"

    qpy.generate_config(args.config_dir)

    project = define_project(config_dir=args.config_dir, project_name=args.project_name)
    data = query_data(args.query)
    run_profiler(data, project, batch_name, run_name)
    qpy.cli.produce_batch_report_cli(
        args.config_dir, args.project_name, batch_name, run_name=run_name
    )
