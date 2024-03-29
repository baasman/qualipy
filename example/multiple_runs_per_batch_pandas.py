import subprocess

from qualipy.backends.pandas_backend.pandas_types import FloatType, ObjectType
import qualipy as qpy


def qualipy_pipeline(configuration_directory="~/flat-data"):
    # load the example dataset
    df = qpy.datasets.load_dataset("flat_data")

    # generate config, can also be done through "qualipy generate-config"
    qpy.generate_config(configuration_directory)

    # Define a simple function
    @qpy.function(return_format=float)
    def mean(data, column):
        return data[column].mean()

    # create a mapping. In this case, since the data is in a flat format, the column values
    # will depend on the values of another column
    value_column = qpy.column(
        column_name="value",
        column_type=FloatType(),
        functions=[mean],
        overwrite_type=True,
        column_stage_collection_name="num_col",
    )
    # we will only use this column when looking at the categorical data
    value_cat_column = qpy.column(
        column_name="value",
        column_type=ObjectType(),
        functions=[],
        is_category=True,
        column_stage_collection_name="cat_col",
    )

    # set up project and add all mappings
    project = qpy.Project(project_name="flat_data", config_dir=configuration_directory)
    project.add_column(value_column, name="num_col")
    project.add_column(value_cat_column, name="cat_col")

    qualipy = qpy.Qualipy(project=project)

    df_var_1 = qpy.backends.pandas_backend.dataset.PandasData(
        df[df.variable == "var_1"]
    )
    # note, we only want to use the numeric column we added. We use the name we defined
    # when adding the column to the project
    qualipy.set_chunked_dataset(
        df_var_1,
        time_column="datetime",
        time_freq="1D",
        run_name="var_1",
        columns=["num_col"],
    )
    # It's useful to set autocommit to False, so that all data gets committed at once, once all data
    # has been processed
    qualipy.run(autocommit=False)

    df_var_2 = qpy.backends.pandas_backend.dataset.PandasData(
        df[df.variable == "var_2"]
    )
    qualipy.set_chunked_dataset(
        df_var_2,
        time_column="datetime",
        time_freq="1D",
        run_name="var_2",
        columns=["num_col"],
    )
    qualipy.run(autocommit=False)

    df_var_3 = qpy.backends.pandas_backend.dataset.PandasData(
        df[df.variable == "var_3"]
    )
    qualipy.set_chunked_dataset(
        df_var_3,
        time_column="datetime",
        time_freq="1D",
        run_name="var_3",
        columns=["cat_col"],
    )
    qualipy.run(autocommit=False)

    # commit manually
    qualipy.commit()


if __name__ == "__main__":
    config_dir = "~/flat_data"
    qualipy_pipeline(config_dir)
    # generate anomaly report
    qpy.cli.produce_anomaly_report_cli(config_dir, "flat_data", run_anomaly=True)