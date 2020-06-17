## Qualipy

Qualipy is a data monitoring and quality library.

## Install

### from Git
```bash
git clone https://github.com/baasman/qualipy.git
pip install .
```
### from pip
```bash
pip install qualipy
```

## At a glance

Qualipy allows you to set constraints on any particular column of a dataframe,
and run data quality aggregates on that column.

To monitor a column, all we need to do is create a mapping to that column. Consider the following script: 

```python
from qualipy import Column, function, Project, Qualipy
from qualipy.backends.pandas_backend.pandas_types import FloatType

import pandas as pd
import numpy as np


@function(return_format=float)
def mean(data, column):
    return data[column].mean()


@function(return_format=float)
def percentage_missing(data, column):
    return data[column].isnull().sum() / data.shape[0]


class MyCol(Column):

    column_name = "my_col"
    column_type = FloatType()
    force_type = True
    null = False
    force_null = False
    unique = False

    functions = [
        {"function": mean, "parameters": {}},
        {"function": percentage_missing, "parameters": {}},
    ]


project = Project(project_name="example", config_dir="/tmp/.qualipy")
project.add_column(MyCol())

dt_range = pd.date_range("2019-01-01", "2019-02-01", freq="1D")
for dt in dt_range:
    data = pd.DataFrame({"my_col": np.random.normal(10, 1, 10)})
    data.loc[np.random.choice([True, False], size=data.shape[0]), "my_col"] = np.NaN
    ds = DataSet(project=project, batch_name=str(dt), time_of_run=dt)
    ds.set_dataset(data)
    ds.run()
```

### What just happened?

First, we created a function called 'mean', using the function decorator. This establishes a numerical aggregator that 
returns the mean of a column, in a float format. We'll do the same with the function 'std'.

Second, we map a class called 'MyCol' to the column 'my_col' by inheriting from the Column base class.
Within that mapping, we specify:
  - column_name: The column it refers to
  - column_type: The type the column should adhere to
  - force_type: If True, process fails if column type does not match
  - null: Can column be null
  - force_null: if null is False, process fails if null values found in column
  - unique: Should uniqueness of column be enforced?
  - functions: The arbitrary functions we'd like to call on the column
  
Third, we establish a Project. A project in Qualipy's case is a representation of the dataset we want to track.
It tracks all columns and tables that belong to the data itself, and connects it to a specific configuration.

Lastly, we specify a specific batch to the project. In this case, we'll be performing data quality measures on our randomly
generated dataset, and give it a batch name to identify it in the dashboard.

## Running the webapp
```bash
qualipy run --config_dir /tmp/.qualipy
```
See `qualipy run --help` for help

## Training the anomaly models
Qualipy will train anomaly models for each numerical aggregate you are tracking. For now,
Qualipy will only train a simplistic Isolation Forest, though more models
are planned for improved anomaly catching.
```bash
qualipy train-anomaly --project_name example --config_dir /tmp/.qualipy
```
See `qualipy train-anomaly --help` for help

