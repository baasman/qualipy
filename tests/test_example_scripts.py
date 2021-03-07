import os
import tempfile
import importlib.util
import subprocess
import shutil
import json

import pytest
import sqlalchemy as sa

import qualipy as qpy


def get_module_from_path(path):
    spec = importlib.util.spec_from_file_location("test_path.test_script", path)
    test_script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(test_script)
    return test_script


def test_profile_dataset_pandas():
    test_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        "example",
        "profile_dataset_pandas.py",
    )
    tmp_config_path = os.path.join(
        tempfile.gettempdir(), next(tempfile._get_candidate_names())
    )
    test_script = get_module_from_path(test_path)
    test_script.qualipy_pipeline(tmp_config_path)

    qpy.cli.produce_batch_report_cli(
        tmp_config_path, "eye_state", "eye-state-run-0", run_name="full-run"
    )

    reports_dir = os.path.join(tmp_config_path, "reports", "profiler")
    reports = list(os.listdir(reports_dir))
    assert len(reports) > 0

    with open(os.path.join(tmp_config_path, "config.json"), "r") as f:
        config = json.load(f)

    engine = sa.create_engine(config["QUALIPY_DB"])
    with engine.connect() as conn:
        ret = conn.execute("select * from eye_state limit 1").fetchall()

    assert len(ret) > 0

    shutil.rmtree(tmp_config_path)


def test_table_dataset_pandas():
    test_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        "example",
        "pandas_table.py",
    )
    tmp_config_path = os.path.join(
        tempfile.gettempdir(), next(tempfile._get_candidate_names())
    )
    test_script = get_module_from_path(test_path)
    test_script.qualipy_pipeline(tmp_config_path)

    qpy.cli.produce_batch_report_cli(
        tmp_config_path, "eye_state", "eye-state-run-0", run_name="full-run"
    )

    reports_dir = os.path.join(tmp_config_path, "reports", "profiler")
    reports = list(os.listdir(reports_dir))
    assert len(reports) > 0

    with open(os.path.join(tmp_config_path, "config.json"), "r") as f:
        config = json.load(f)

    engine = sa.create_engine(config["QUALIPY_DB"])
    with engine.connect() as conn:
        ret = conn.execute("select * from eye_state limit 1").fetchall()

    assert len(ret) > 0

    shutil.rmtree(tmp_config_path)


def test_table_dataset_pandas_from_config():
    test_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        "example",
        "pandas_table_from_config.py",
    )
    tmp_config_path = os.path.join(
        tempfile.gettempdir(), next(tempfile._get_candidate_names())
    )
    test_script = get_module_from_path(test_path)
    test_script.qualipy_pipeline(tmp_config_path)

    qpy.cli.produce_batch_report_cli(
        config_dir=tmp_config_path,
        project_name="eye_state",
        batch_name="eye-state-run-0",
        run_name="full-run",
    )

    reports_dir = os.path.join(tmp_config_path, "reports", "profiler")
    reports = list(os.listdir(reports_dir))
    assert len(reports) > 0

    with open(os.path.join(tmp_config_path, "config.json"), "r") as f:
        config = json.load(f)

    engine = sa.create_engine(config["QUALIPY_DB"])
    with engine.connect() as conn:
        ret = conn.execute("select * from eye_state limit 1").fetchall()

    assert len(ret) > 0

    shutil.rmtree(tmp_config_path)


def test_chunked_dataset_anomaly_pandas():
    test_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        "example",
        "chunked_dataset_anomaly_pandas.py",
    )
    tmp_config_path = os.path.join(
        tempfile.gettempdir(), next(tempfile._get_candidate_names())
    )
    test_script = get_module_from_path(test_path)
    test_script.qualipy_pipeline(tmp_config_path)

    qpy.cli.produce_anomaly_report_cli(
        config_dir=tmp_config_path, project_name="stocks", run_anomaly=True
    )

    reports_dir = os.path.join(tmp_config_path, "reports", "anomaly")
    reports = list(os.listdir(reports_dir))

    assert len(reports) > 0

    with open(os.path.join(tmp_config_path, "config.json"), "r") as f:
        config = json.load(f)

    engine = sa.create_engine(config["QUALIPY_DB"])
    with engine.connect() as conn:
        ret = conn.execute("select * from stocks limit 1").fetchall()

    assert len(ret) > 0

    shutil.rmtree(tmp_config_path)


def test_multiple_runs_per_batch_pandas():
    test_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        os.pardir,
        "example",
        "multiple_runs_per_batch_pandas.py",
    )
    tmp_config_path = os.path.join(
        tempfile.gettempdir(), next(tempfile._get_candidate_names())
    )
    test_script = get_module_from_path(test_path)
    test_script.qualipy_pipeline(tmp_config_path)

    qpy.cli.produce_anomaly_report_cli(
        config_dir=tmp_config_path, project_name="flat_data", run_anomaly=True
    )

    reports_dir = os.path.join(tmp_config_path, "reports", "anomaly")
    reports = list(os.listdir(reports_dir))

    assert len(reports) > 0

    with open(os.path.join(tmp_config_path, "config.json"), "r") as f:
        config = json.load(f)

    engine = sa.create_engine(config["QUALIPY_DB"])
    with engine.connect() as conn:
        ret = conn.execute("select * from flat_data limit 1").fetchall()

    assert len(ret) > 0

    shutil.rmtree(tmp_config_path)


if __name__ == "__main__":
    test_table_dataset_pandas_from_config()