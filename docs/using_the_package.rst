.. highlight:: sh

=================
Using the package
=================


Creating Functions
-------------------

A ``function`` allows us to create any arbitrary aggregate or measure on a column. There are several rules
to creating a function.
  - The two arguments must always be ``data`` and ``column``. When you write your function, ``data`` represents
the data object (like a pandas dataframe), and ``column`` is the string name representing the column you
are applying the method to.
  - The returned object must adhere to the stated return format, which will be clear in the following example.
::

    from qualipy import function
    @function(return_format=float)
    def mean(data, column):
        return data[column].mean()

Per the rules, ``data`` represents the pandas DataFrame, the string name of column is used to access the column
from the DataFrame. Additionally, the method ``mean`` returns a float value, which is consistent with the
``return_format`` set in the decorator call.

There are several parameters one can use when defining a ``function``.

====================== ============================================================
`return_format`        What the function should return. Can be either float, int,
                       str, bool, or dict
`allowed_arguments`    An optional list of strings that specify what arguments can
                       be passed to the function, in addition to ``data`` and
                       ``function``
`fail`                 Can be True or False. If using return format of bool, should the process stop if
                       the function returns ``False``?
====================== ============================================================

Creating a mapping
-------------------

The easiest way to apply rules to a column is to create a class that inherits from qualipy.Column.
