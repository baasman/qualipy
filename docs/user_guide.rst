.. highlight:: sh

=================
User Guide
=================


Creating Functions
-------------------

.. currentmodule:: qualipy.reflect.function

.. autofunction:: function

Example 1 - A simple function with no additional arguments::

    from qualipy.reflect.function import function
    @function(return_format=float)
    def mean(data, column):
        return data[column].mean()

Per the rules, ``data`` represents the data passed through, in this case a pandas DataFrame,
 ``column`` is the string name of column is used to access the column from the DataFrame. 
Additionally, the method ``mean`` returns a float value, which is consistent with the
``return_format`` set in the decorator call.

Example 2 - A simple function with additional arguments::

    @function(return_format=int, allowed_arguments=["standard_deviations"])
    def std_over_limit(data, column, standard_deviations):
        mean = data[column].mean()
        std = data[column].std()
        data = data[
            (data[column] < (mean - standard_deviations * std))
            | (data[column] > (mean + standard_deviations * std))
        ]
        return data.shape[0]


Example 3 - A function when running SQL as backend::

    @function(return_format=float)
    def mean(data, column):
        return data.engine.execute(
            sa.select([sa.func.avg(sa.column(column))]).select_from(data._table)
        ).scalar()


Creating a mapping
-------------------

.. currentmodule:: qualipy.reflect.column

.. autofunction:: column

Example 1 - Reflect a pandas column with one function::

    price = column(column_name="price", column_type=FloatType(), functions=[mean])

Here, ``price`` is the name of the pandas column. We want to column to be of float type,
and we're collecting the mean of the price.

Example 2 - Reflect a column, and call a function with arguments::

    price = column(
        column_name="price",
        column_type=FloatType(),
        functions=[{"function": std_over_limit, "parameters": {"standard_deviations": 3}}],
    )

Example 3 - Reflect multiple columns, and call a function on just one of them::

    num_columns = column(
        column_name=["price", "some_other_column"],
        column_type=FloatType(),
        functions=[mean],
        extra_functions={
            "price": [
                {"function": std_over_limit, "parameters": {"standard_deviations": 3}},
            ],
        },
    )

In this scenario, ``mean`` will be applied to ``price``, but ``std_over_limit`` will only
be applied ``price``

Project
--------

.. currentmodule:: qualipy.project

.. autoclass:: Project     

   .. automethod:: __init__
   .. automethod:: add_column

Example 1 - Instantiate a project::

    from qualipy.project import Project

    project = Project(project_name='stocks', config_dir='/tmp/.config')

Example 2 - Instantiate a project and add a column to it::

    from qualipy.project import Project

    project = Project(project_name='stocks', config_dir='/tmp/.config')
    # using the price column defined above
    project.add_column(column=price, name='price_analysis')

Supported DataSet Types
------------------------

Currently, there are three different dataset types supported: Pandas, Spark, and SQL

**Pandas**

.. currentmodule:: qualipy.backends.pandas_backend.dataset

.. autoclass:: PandasData     

   .. automethod:: __init__
   .. automethod:: set_stratify_rule

Example 1 - Setting symbol as a stratification::

    from qualipy.backends.pandas_backend.dataset import PandasData

    stocks = PandasData(stocks)
    stocks.set_stratify_rule("symbol")

Example 2 - Setting symbol as a stratification and specifying the subset of stocks to analyze::

    from qualipy.backends.pandas_backend.dataset import PandasData

    stocks = PandasData(stocks)
    stocks.set_stratify_rule("symbol", values=['IBM', 'AAPL'])

**SQL**

.. currentmodule:: qualipy.backends.sql_backend.dataset

.. autoclass:: SQLData     

   .. automethod:: __init__
   .. automethod:: set_custom_where

Example 1 - Instantiating a table::

    import sqlalchemy as sa
    from qualipy.backends.sql_backend.dataset import SQLData

    engine = sa.create_engine('sqlite://')
    data = SQLData(engine=engine, table_name='my_table')

Example 2 - Instantiating a table and setting a custom where clause::

    import sqlalchemy as sa
    from qualipy.backends.sql_backend.dataset import SQLData

    engine = sa.create_engine('sqlite://')
    data = SQLData(engine=engine, table_name='my_table')
    data.set_custom_where("my_col = 'setosa'")

Qualipy
--------

.. currentmodule:: qualipy.run

.. autoclass:: Qualipy     

   .. automethod:: __init__
   .. automethod:: set_dataset
   .. automethod:: set_chunked_dataset


Data types
-----------

There are several data types one can check for, depending on the backend.
For pandas, these include
    * `DateTimeType`
    * `FloatType` - will match against float16-128
    * `IntType` - will match against int0-64
    * `NumericTypeType` - will match with any numeric subtype
    * `ObjectType`
    * `BoolType`
