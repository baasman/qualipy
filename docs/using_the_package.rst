.. highlight:: sh

=================
Using the package
=================


Creating a mapping
-------------------

The easiest way to apply rules to a column is to create a class that inherits from qualipy.Column.

There are several attributes one can set on this class.

====================== =============== ============================================================
`column_name`          str              The name of the column in the pandas or Spark DataFrame

`column_type`                           Instance of the type the column should adhere too. See *** for more info

`force_type`           bool             If column does not adhere to the above type, should the process fail?

`overwrite_type`       bool             This option will convert the datatype before calling a function with the supposed datatype

`null`                 bool             Can the column contain missing values?

`force_null`           bool             If `null` is set to False, and this to `True`, the process will fail
                                        if it encounters missing values

`unique`               bool             Should uniqueness in the column be enforced?

`is_category`          bool             Should this column be encoded as a categorical variable?

`functions`            List[callable]   This is a list of callable functions, or function specifications.
                                        See the next section on how to call a function

`extra_functions`      Dict[str,        This is a list of callable functions, or function specifications.
                       List[callable]]  See the next section on how to call a function

====================== =============== ============================================================


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


Calling functions in a mapping
-------------------------------

For the `functions` attribute, there are two ways in which two call a function, depending
on whether or not there are parameters

If not parameters are needed, are the defaults suffice, you can simply list the callable functions (any function
with the proper decorator)::

    ...
    functions = [mean, std]
    ...

If there are parameters, you can give them as such::

    ...
    functions = [{'function': mean, 'parameters': {'param1': 'some_value'}}]
    ...

In case you are specifying your mapping for multiple columns, but only want to call a function on one
of them, you can use the `extra_functions` attribute to specify a list of functions just for that column::

    ...
    extra_functions = {'my_col': [{'function': mean, 'parameters': {'param1': 'some_value'}}]}
    ...


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
