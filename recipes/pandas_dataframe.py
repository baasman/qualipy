import datetime

import qualipy as qpy
from qualipy.reflect.column import column
from qualipy.backends.pandas_backend.dataset import PandasData
from qualipy.backends.pandas_backend.pandas_types import (
    ObjectType,
    FloatType,
    DateTimeType,
)

import sqlalchemy as sa
import pandas as pd


@qpy.function(return_format=float)
def mean(data, column):
    return data[column].mean()


def read_data(path):
    data = pd.read_csv(path)
    return data


### SET THIS ###
num_cols = []

cat_cols = []

date_cols = []


def define_project(config_dir, project_name):
    num_columns = []
    for col in num_cols:
        num_columns.append(
            column(column_name=col, column_type=FloatType(), functions=[mean])
        )

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


def run_time_series_analysis(data, project, time_column, time_freq="1D", run_name=None):
    batch = qpy.Qualipy(
        project=project,
        backend="pandas",
        time_of_run=datetime.datetime.now(),
        batch_name=batch_name,
    )

    finding_data = PandasData(data)
    batch.set_chunked_dataset(finding_data, time_column=time_column, time_freq="1D")
    batch.run(autocommit=False)
    batch.commit()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("config_dir", type=str)
    parser.add_argument("project_name", type=str)
    parser.add_argument("query", type=str)
    parser.add_argument("--batch_name", type=str)
    parser.add_argument("--run_name", type=str)
    parser.add_argument("--time_column", type=str)
    parser.add_argument("--time_freq", type=str)

    args = parser.parse_args()

    batch_name = (
        args.batch_name if args.batch_name is not None else f"{args.project_name}_full"
    )
    run_name = args.run_name if args.run_name is not None else f"{args.project_name}"

    qpy.generate_config(args.config_dir)

    project = define_project(config_dir=args.config_dir, project_name=args.project_name)
    data = read_data(args.query)

    # profiler analysis
    run_profiler(data, project, batch_name, run_name)
    qpy.cli.produce_batch_report_cli(
        args.config_dir, args.project_name, batch_name, run_name=run_name
    )

    # time series analysis
    if args.time_column is not None:
        time_freq = args.time_freq if args.time_freq is not None else "1D"
        run_time_series_analysis(data, project, args.time_column, time_freq=time_freq)
