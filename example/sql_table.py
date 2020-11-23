import qualipy as qpy

import sqlalchemy as sa


def qualipy_pipeline(configuration_directory="~/eye-state"):
    # set up the config
    qpy.generate_config(configuration_directory)

    eye_state = qpy.datasets.load_dataset("eye_state")

    engine = sa.create_engine("sqlite:////home/baasman/Qualipy/examples/eye_state.db")

    # Define a simple function
    @qpy.function(return_format=float)
    def mean(data, column):
        return data[column].mean()

    # Set up the table definition. Here we will let Qualipy do all the inference
    # of types and names. The only thing we need to provide is a sample dataset
    table = qpy.sql_table(table_name="eye_state", engine=engine, functions=[mean])

    # set up project and add all mappings
    project = qpy.Project(project_name="eye_state", config_dir=configuration_directory)
    project.add_table(table)

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