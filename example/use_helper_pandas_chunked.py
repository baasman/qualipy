from qualipy.helper.auto_qpy import setup_auto_qpy, auto_qpy_chunked
import qualipy as qpy


def qualipy_pipeline(configuration_directory, project_name):

    stocks = qpy.datasets.load_dataset("stocks")

    project = setup_auto_qpy(
        data=stocks,
        configuration_dir=configuration_directory,
        project_name=project_name,
    )
    auto_qpy_chunked(
        stocks, project, time_column="date", time_freq="6M", stratify="symbol"
    )


if __name__ == "__main__":
    config_dir = "~/stocks-from-helper"
    project_name = "stocks_helper"
    qualipy_pipeline(configuration_directory=config_dir, project_name=project_name)