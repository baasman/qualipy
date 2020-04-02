.. highlight:: sh

========
Qualipy
========

Qualipy is a data monitoring and quality library. Most data quality tools out
there focus on monitoring the input and/or the output. Qualipy, on the other hand,
is completely focused on understanding your data during a pipeline or data workflow.


What it looks like
===================

Take a look at the following example to get a view of what a qualipy pipeline looks like::

    from qualipy import Column, function, Project, DataSet
    from qualipy.backend.pandas_backend.pandas_types import FloatType

    import pandas as pd


    @function(return_format=float)
    def mean(data, column):
        return data[column].mean()


    class SepalLength(Column):

        column_name = 'sepal.length'
        column_type = FloatType()
        force_type = True
        null = False
        force_null = False
        unique = False

        functions = [
            {
                'function': mean,
                'parameters': {}
            }
        ]

    project = Project(project_name='iris')
    project.add_column(SepalLength())

    df = pd.read_csv('https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv')

    ds = DataSet(project=project, batch_name='test-batch')
    ds.set_dataset(df)
    ds.run()


What just happened?
====================

First, we created a function called 'mean', using the function decorator. This establishes a numerical aggregator that
returns the mean of a column, in a float format.

Second, we map a class called 'SepalLength' to the column 'sepal.length' by inheriting from the Column base class.
Within that mapping, we specify:
  - column_name: The column it refers to
  - column_type: The type the column should adhere to
  - force_type: If True, process fails if column type does not match
  - null: Can column be null
  - force_null: if null is False, process fails if null values found in column
  - unique: Should uniqueness of column be enforced?
  - functions: The arbitrary functions we'd like to call on the

Third, we establish a Project. This project will be the overarching chapter to contain and store each batch's informations.
Additionally, it allows us to specify where to store the resulting metadata and configuration.

Lastly, we specify a specific batch to the project. In this case, we'll be performing data quality measures on Iris,
and give it a batch name to identify it in the dashboard.
