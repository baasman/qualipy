import subprocess

from qualipy.backends.pandas_backend.pandas_types import FloatType, ObjectType
import qualipy as qpy
from qualipy.cli.report.util import produce_anomaly_report_cli


def qualipy_pipeline(configuration_directory="~/stocks"):

    # generate config, can also be done through "qualipy generate-config"
    qpy.generate_config(configuration_directory)

    # load data
    stocks = qpy.datasets.load_dataset("stocks")

    # Define a simple function
    @qpy.function(return_format="custom", custom_value_return_format=float)
    def mean_with_meta(data, column):
        return [
            {
                "value": data[column].mean(),
                "run_name": "something",
                "meta": {"x": "y"},
            }
        ]

    # create a mapping
    price_column = qpy.column(
        column_name="price", column_type=FloatType(), functions=[mean_with_meta]
    )

    # set up project and add all mappings
    project = qpy.Project(project_name="stocks", config_dir=configuration_directory)
    project.add_column(price_column)

    # instantiate qualipy object
    qualipy = qpy.Qualipy(project=project)

    # instantiate pandas dataset, and stratify by the symbol variable
    stocks = qpy.backends.pandas_backend.dataset.PandasData(stocks)
    stocks.set_stratify_rule("symbol")

    # since we already have all of the data, we will "chunk" the dataset by time,
    # and simulate the data as if we were examing 6 hour batches
    qualipy.set_chunked_dataset(
        stocks, time_column="date", time_freq="6M", run_name="stocks"
    )
    qualipy.run(autocommit=True)


if __name__ == "__main__":
    config_dir = "~/stocks"
    qualipy_pipeline(config_dir)

    # generate the anomaly report. See qualipy produce-anomaly-report -h for more info
    produce_anomaly_report_cli(config_dir, "stocks", run_anomaly=False)