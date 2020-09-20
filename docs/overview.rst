.. highlight:: sh

=========
Overview
=========

What it looks like
===================

Take a look at the following example to get a view of what a qualipy pipeline looks like::

    from qualipy.reflect.column import column
    from qualipy.reflect.function import function
    from qualipy import Qualipy, Project
    from qualipy.backends.pandas_backend.pandas_types import FloatType
    from qualipy.backends.pandas_backend.dataset import PandasData
    from qualipy.datasets import stocks


    @function(return_format=float)
    def mean(data, column):
        return data[column].mean()


    price_column = column(column_name="price", column_type=FloatType(), functions=[mean])


    project = Project(project_name="example", config_dir="/tmp/.qualipy")
    project.add_column(price_column)

    qualipy = Qualipy(project=project)
    stocks = PandasData(stocks)
    stocks.set_stratify_rule("symbol")
    qualipy.set_chunked_dataset(stocks, time_column="date", time_freq="1M")
    qualipy.run(autocommit=True)


What just happened?
====================

First, we created a function called 'mean', using the function decorator. This establishes a numerical aggregator that
returns the mean of a column, in a float format. This could now be applied to any live - numerical
data and tracked.

Second, we create a mapping between the column "price" of the stocks data and the 
`price_column` object. This establishes the mapping, enforces a datatype, and specifies
what metrics to track. Many more options are available.

Third, we establish a Project. This project will be the overarching object that persists each batch's aggregate data.

Now that we've set up the boilerplate of Qualipy, we can get to actually running it
on some real data. All we need to do instantiate the Qualipy object and tie it to whatever
project we want to track. We also need to instantiate our dataset, in this case a pandas
dataset, and optionally define a way to stratify in incoming data. All that's left is to
set the current dataset, and run. 
