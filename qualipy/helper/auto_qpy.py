import datetime
from typing import Union, List, Dict

import pandas as pd
import sqlalchemy as sa

import qualipy as qpy


def setup_auto_qpy(
    data: pd.DataFrame,
    configuration_dir: str,
    project_name: str,
    functions: list = None,
    types: dict = None,
    overwrite_type: bool = False,
    ignore: list = None,
    int_as_cat: Union[bool, int] = 25,
    split_on: str = None,
    column_stage_collection_name: str = None,
):
    qpy.generate_config(configuration_dir, create_in_empty_dir=True)
    project = qpy.Project(project_name=project_name, config_dir=configuration_dir)
    if split_on:
        full_table = qpy.reflect.table.Table(table_name=project_name, columns=[])
        # NOTE: this is not working because the project overwrites the key
        for name, group in data.groupby(split_on):
            new_table = qpy.pandas_table(
                infer_schema=True,
                sample_dataset=group,
                functions=functions,
                types=types,
                overwrite_type=overwrite_type,
                ignore=ignore,
                int_as_cat=int_as_cat,
                split_on=[split_on, name],
                column_stage_collection_name=column_stage_collection_name,
            )
            project.add_table(new_table)
    else:
        full_table = qpy.pandas_table(
            infer_schema=True,
            sample_dataset=data,
            functions=functions,
            types=types,
            overwrite_type=overwrite_type,
            ignore=ignore,
            int_as_cat=int_as_cat,
        )
        project.add_table(full_table)
    return project


def setup_auto_qpy_sql_table(
    table_name: str,
    engine: sa.engine.base.Engine,
    configuration_dir: str,
    project_name: str,
    schema: str = None,
    columns: List[str] = None,
    functions: list = None,
    extra_functions: Dict[str, list] = None,
    types: dict = None,
    overwrite_type: bool = False,
    ignore: list = None,
    int_as_cat: Union[bool, int] = 25,
    split_on: str = None,
    column_stage_collection_name: str = None,
):
    qpy.generate_config(configuration_dir, create_in_empty_dir=True)
    project = qpy.Project(project_name=project_name, config_dir=configuration_dir)
    full_table = qpy.sql_table(
        table_name=table_name,
        engine=engine,
        schema=schema,
        columns=columns,
        functions=functions,
        extra_functions=extra_functions,
        ignore=ignore,
        int_as_cat=int_as_cat,
        bool_as_cat=True,
    )
    project.add_table(full_table)
    return project


def auto_qpy_profiler(data: pd.DataFrame, project: qpy.Project, stratify: str = None):
    batch_name = "from-auto"
    run_name = "auto-run"
    qualipy = qpy.Qualipy(project=project, batch_name=batch_name)

    qpy_data = qpy.backends.pandas_backend.dataset.PandasData(data)
    if stratify is not None:
        qpy_data.set_stratify_rule(stratify)
    qualipy.set_dataset(qpy_data, run_name=run_name)

    qualipy.run(autocommit=True, profile_batch=True)

    if stratify is not None:
        run_name = [f"{run_name}_{strat}" for strat in data[stratify].unique().tolist()]

    qpy.cli.produce_batch_report_cli(
        config_dir=project.config_dir,
        project_name=project.project_name,
        batch_name=batch_name,
        run_name=run_name,
    )


def auto_qpy_chunked(
    data: pd.DataFrame,
    project: qpy.Project,
    time_column: str,
    time_freq: str = "1D",
    stratify: str = None,
    run_anomaly: bool = True,
    run_name: str = None,
    out_file: str = None,
):
    qualipy = qpy.Qualipy(project=project)

    if run_name is None:
        run_name = "auto-qpy"

    qpy_data = qpy.backends.pandas_backend.dataset.PandasData(data)
    if stratify is not None:
        qpy_data.set_stratify_rule(stratify)
    qualipy.set_chunked_dataset(
        qpy_data, run_name=run_name, time_column=time_column, time_freq=time_freq
    )

    qualipy.run(autocommit=True)
    qpy.cli.produce_anomaly_report_cli(
        config_dir=project.config_dir,
        project_name=project.project_name,
        run_anomaly=run_anomaly,
        run_name=run_name,
        out_file=out_file,
    )


def auto_qpy_single_batch(
    data: pd.DataFrame,
    project: Union[qpy.Project, str],
    configuration_dir: str = None,
    stratify: str = None,
    run_anomaly: bool = True,
    run_name: str = None,
    commit: bool = True,
    column_collection_name: List[str] = None,
    produce_report: bool = True,
):
    if isinstance(project, str):
        project = qpy.Project(
            project_name=project, config_dir=configuration_dir, re_init=True
        )
    qualipy = qpy.Qualipy(project=project)

    if run_name is None:
        run_name = "auto-qpy"

    qpy_data = qpy.backends.pandas_backend.dataset.PandasData(data)
    if stratify is not None:
        qpy_data.set_stratify_rule(stratify)
    qualipy.set_dataset(qpy_data, run_name=run_name, columns=column_collection_name)

    qualipy.run(autocommit=commit)

    if produce_report:
        qpy.cli.produce_anomaly_report_cli(
            config_dir=project.config_dir,
            project_name=project.project_name,
            run_anomaly=run_anomaly,
            run_name=run_name,
        )
    return qualipy


def auto_qpy_single_batch_sql(
    table_name: str,
    engine: sa.engine.base.Engine,
    project: Union[qpy.Project, str],
    configuration_dir: str,
    schema: str = None,
    stratify: str = None,
    run_anomaly: bool = True,
    run_name: str = None,
    commit: bool = True,
    column_collection_name: List[str] = None,
    produce_report: bool = True,
):
    if isinstance(project, str):
        project = qpy.Project(
            project_name=project, config_dir=configuration_dir, re_init=True
        )
    qualipy = qpy.Qualipy(project=project, backend="sql")

    if run_name is None:
        run_name = "auto-qpy"

    qpy_data = qpy.backends.sql_backend.dataset.SQLData(
        engine=engine, table_name=table_name, schema=schema
    )
    qualipy.set_dataset(qpy_data, run_name=run_name, columns=column_collection_name)

    qualipy.run(autocommit=commit)

    if produce_report:
        qpy.cli.produce_anomaly_report_cli(
            config_dir=project.config_dir,
            project_name=project.project_name,
            run_anomaly=run_anomaly,
            run_name=run_name,
        )
    return qualipy