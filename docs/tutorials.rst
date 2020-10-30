.. highlight:: sh

======================
Tutorials and Recipes
======================

For beginners getting into using Qualipy, I'd recommend following these tutorials in order

Tutorial 1 
-----------
Explore a dataset by collecting aggregates over chunks of a dataset: Example_1_.

.. _Example_1: https://github.com/baasman/qualipy/blob/qualipy-0.1.1/example/chunked_dataset_anomaly_pandas.py

Concepts:

1. Creating a Pandas function
2. Defining core components in Qualipy
3. Simulate a time series by chunking the input data


Tutorial 2 
-----------
Explore a single batch of data using Qualipy: Example_2_.

.. _Example_2: https://github.com/baasman/qualipy/blob/qualipy-0.1.1/example/profile_dataset_pandas.py

Concepts:

1. Creating a Pandas function
2. Defining core components in Qualipy
3. Profiling a dataset by automatically collecting useful information about a single batch of data

Tutorial 3
-----------
Similar to example 1, but this is a more involved example and includes flat data as a source: Example_3_.

.. _Example_3: https://github.com/baasman/qualipy/blob/qualipy-0.1.1/example/multiple_runs_per_batch_pandas.py

Concepts:

1. Creating a Pandas function
2. Using reference names for columns to refer to them at different times
3. Working with numerical and categorical data
4. Simulate a time series by chunking the input data


Tutorial 4
-----------
Like example 3, but creates a new more complex function: Example_4_.

.. _Example_4: https://github.com/baasman/qualipy/blob/qualipy-0.1.1/example/complex_functions_pandas.py

Concepts:

1. Creating a Pandas function
2. Creating another Pandas function with an additional argument
3. Specifying the argument in the column definition
4. Using reference names for columns to refer to them at different times
5. Working with numerical and categorical data
6. Simulate a time series by chunking the input data


Recipe 1
---------
Use this to analyze a pandas dataframe. All you need to fill in is the numeric, categorical,
and datetime columns, and the rest should work like magic!

.. _Recipe_1: https://github.com/baasman/qualipy/blob/qualipy-0.1.1/recipes/pandas_dataframe.py

