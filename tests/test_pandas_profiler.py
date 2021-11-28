import os

from qualipy.helper.auto_qpy import setup_auto_qpy, auto_qpy_profiler
import qualipy as qpy


def test_pandas_profiler_defaults(tmp_dir):

    eye_state = qpy.datasets.load_dataset("eye_state")

    project = setup_auto_qpy(
        data=eye_state,
        configuration_dir=tmp_dir,
        project_name="test",
    )
    auto_qpy_profiler(eye_state, project)

    reports_dir = os.path.join(tmp_dir, "reports", "profiler")
    reports = list(os.listdir(reports_dir))
    assert len(reports) > 0


def test_pandas_profiler_with_custom_function(tmp_dir):

    eye_state = qpy.datasets.load_dataset("eye_state")

    @qpy.function(return_format=float)
    def mean(data, column):
        return data[column].mean()

    project = setup_auto_qpy(
        data=eye_state,
        configuration_dir=tmp_dir,
        project_name="test",
    )
    auto_qpy_profiler(eye_state, project)

    reports_dir = os.path.join(tmp_dir, "reports", "profiler")
    reports = list(os.listdir(reports_dir))
    assert len(reports) > 0