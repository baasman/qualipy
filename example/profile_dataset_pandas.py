from qualipy.backends.pandas_backend.pandas_types import FloatType, ObjectType
import qualipy as qpy

import pandas as pd

from util import get_project_data


def qualipy_pipeline(configuration_directory="~/eye-state"):

    # Define a simple function
    @qpy.function(return_format="custom", custom_value_return_format=float)
    def mean(data, column):
        return [
            {"value": data[column].mean(), "run_name": "t1"},
            {"value": data[column].mean(), "run_name": "t2"},
        ]

    eye_state = qpy.datasets.load_dataset("eye_state")

    project = qpy.helper.setup_auto_qpy(
        sample_data=eye_state,
        configuration_dir=configuration_directory,
        project_name="eye_state",
        functions=[mean],
        types={"Class": ObjectType()},
    )

    # instantiate qualipy object. Setting a batch name will make it easy to identify
    # when generating batch report
    qualipy = qpy.Qualipy(project=project, batch_name="eye-state-run-0")

    eye_state = qpy.backends.pandas_backend.dataset.PandasData(eye_state)
    qualipy.set_dataset(eye_state, run_name="full-run")

    # By setting profile_batch=True, Qualipy will store meta information for this batch,
    # allowing us to construct the batch report by referring to the specific batch and run name
    qualipy.run(autocommit=True, profile_batch=True)


if __name__ == "__main__":
    config_dir = "~/eye-state"
    qualipy_pipeline(config_dir)
    # generate the batch report. See qualipy produce-batch-report -h for more help
    qpy.cli.produce_batch_report_cli(
        config_dir=config_dir,
        project_name="eye_state",
        batch_name="eye-state-run-0",
        run_name="full-run",
    )