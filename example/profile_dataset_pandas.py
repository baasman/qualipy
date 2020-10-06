import subprocess

from qualipy.backends.pandas_backend.pandas_types import FloatType, ObjectType
import qualipy as qpy

import pandas as pd


# set up the config
qpy.generate_config("/tmp/eye-state")

# Define a simple function
@qpy.function(return_format=float)
def mean(data, column):
    return data[column].mean()


# create mappings
my_mappings = []

# Create a mapping for each variable. There are 14 float type variables, for which
# we want to collect the mean, and one categorical variable.
for col in [f"V{i}" for i in range(1, 15)]:
    my_mappings.append(
        qpy.column(column_name=col, column_type=FloatType(), functions=[mean])
    )
# To denote a variable as categorical, make sure to set is_category=True. By default
# Qualipy will assume numeric type when collecting batch information
my_mappings.append(
    qpy.column(column_name="Class", column_type=ObjectType(), is_category=True)
)

# set up project and add all mappings
project = qpy.Project(project_name="eye_state", config_dir="/tmp/eye-state")
for mapping in my_mappings:
    project.add_column(mapping)

# instantiate qualipy object. Setting a batch name will make it easy to identify
# when generating batch report
qualipy = qpy.Qualipy(project=project, batch_name="eye-state-run-0")

eye_state = qpy.datasets.load_dataset("eye_state")

eye_state = qpy.backends.pandas_backend.dataset.PandasData(eye_state)
qualipy.set_dataset(eye_state, run_name="full-run")

# By setting profile_batch=True, Qualipy will store meta information for this batch,
# allowing us to construct the batch report by referring to the specific batch and run name
qualipy.run(autocommit=True, profile_batch=True)

# generate the batch report. See qualipy produce-batch-report -h for more help
subprocess.check_output(
    "qualipy produce-batch-report /tmp/eye-state eye_state eye-state-run-0 --run_name full-run --out_file ~/eye-state-report.html",
    shell=True,
)