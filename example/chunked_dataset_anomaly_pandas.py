import subprocess

from qualipy.backends.pandas_backend.pandas_types import FloatType, ObjectType
import qualipy as qpy

# generate config, can also be done through "qualipy generate-config"
qpy.generate_config("~/stocks")

# load data
stocks = qpy.datasets.load_dataset("stocks")

# Define a simple function
@qpy.function(return_format=float)
def mean(data, column):
    return data[column].mean()


# create a mapping
price_column = qpy.column(
    column_name="price", column_type=FloatType(), functions=[mean]
)

# set up project and add all mappings
project = qpy.Project(project_name="stocks", config_dir="~/stocks")
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

# generate the anomaly report. See qualipy produce-anomaly-report -h for more info
subprocess.check_output(
    "qualipy produce-anomaly-report ~/stocks stocks --run_anomaly true --out_file ~/stocks.html",
    shell=True,
)
