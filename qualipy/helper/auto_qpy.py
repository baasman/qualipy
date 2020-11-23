import datetime

import pandas as pd

import qualipy as qpy


def setup_auto_qpy(
    data: pd.DataFrame,
    configuration_dir: str,
    project_name: str,
    functions: list = None,
    types: dict = None,
    overwrite_type: bool = False,
    ignore: list = None,
):
    qpy.generate_config(configuration_dir)
    table = qpy.pandas_table(
        infer_schema=True,
        sample_dataset=data,
        functions=functions,
        types=types,
        overwrite_type=overwrite_type,
        ignore=ignore,
    )
    project = qpy.Project(project_name=project_name, config_dir=configuration_dir)
    project.add_table(table)
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
    )
