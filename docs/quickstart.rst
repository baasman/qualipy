.. highlight:: sh

===========
Quickstart
===========


Basic Example
==============

First, we will create a function called 'mean', using the function decorator. This establishes a numerical aggregator that
returns the mean of a column, in a float format. We'll do the same with the function 'std'.::

    from qualipy import Column, function, Project, DataSet
    from qualipy.backends.pandas_backend.pandas_types import FloatType

    import pandas as pd
    import numpy as np


    @function(return_format=float)
    def mean(data, column):
        return data[column].mean()


    @function(return_format=float)
    def percentage_missing(data, column):
        return data[column].isnull().sum() / data.shape[0]


Second, we map a class called 'MyCol' to the column 'my_col' by inheriting from the Column base class.
Within that mapping, we specify:
  - column_name: The column it refers to
  - column_type: The type the column should adhere to
  - force_type: If True, process fails if column type does not match
  - null: Can column be null
  - force_null: if null is False, process fails if null values found in column
  - unique: Should uniqueness of column be enforced?
  - functions: The arbitrary functions we'd like to call on the column

For example, to specify a column with the two functions defined above::

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

Third, we establish a Project. A project in Qualipy's case is a representation of the dataset we want to track.
It tracks all columns and tables that belong to the data itself, and connects it to a specific configuration.::

    project = Project(project_name="example", config_dir="/tmp/.qualipy")
    project.add_column(MyCol())

Now we can start the web app using qualipy run.

    $ qualipy run --config_dir /tmp/.qualipy
