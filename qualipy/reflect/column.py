from typing import Any, Dict, Callable, Tuple, List, Union

from qualipy.util import copy_function_spec


def _get_functions(functions, column_name: str = None) -> Tuple[str, Callable]:
    methods = []
    if isinstance(functions, dict):
        if column_name in functions:
            functions = functions[column_name]
        else:
            functions = []
    if functions:
        for func in functions:
            copied_function = copy_function_spec(func)
            methods.append((copied_function.__name__, copied_function))
    return methods


class Column:

    column_name = None
    column_type = None
    force_type = False
    overwrite_type = False
    null = True
    force_null = False
    unique = False
    is_category = False
    functions = []
    extra_functions = []

    def _as_dict(self, name: str, read_functions: bool = True) -> Dict[str, Any]:
        dict_ = {
            "name": self.column_name,
            "type": self.column_type,
            "force_type": self.force_type,
            "overwrite_type": self.overwrite_type,
            "null": self.null,
            "force_null": self.force_null,
            "unique": self.unique,
            "is_category": self.is_category,
            "functions": self._get_functions(column_name=name)
            if read_functions
            else self.functions,
            "extra_functions": self._get_functions("extra_functions", column_name=name),
        }
        return dict_

    def _from_dict(self, args: Dict):
        for key, val in args.items():
            setattr(self, key, val)

    def _get_functions(
        self, fun_attribute: str = "functions", column_name: str = None
    ) -> Tuple[str, Callable]:
        methods = []
        given_methods = getattr(self, fun_attribute, None)
        if fun_attribute == "extra_functions":
            if column_name in given_methods:
                given_methods = given_methods[column_name]
            else:
                given_methods = []
        if given_methods:
            for func in given_methods:
                copied_function = copy_function_spec(func)
                methods.append((copied_function.__name__, copied_function))
        return methods


def column(
    column_name: Union[str, List[str]] = None,
    column_type=None,
    force_type: bool = False,
    overwrite_type: bool = False,
    null: bool = True,
    force_null: bool = False,
    unique: bool = False,
    is_category: bool = False,
    functions: List[Union[Callable, Dict]] = None,
    extra_functions: Dict[str, Dict] = None,
):
    """This allows us to map to a column of a data object.

    This is one of the essential components of Qualipy. Using column ``allows`` us to map
    to a specific column of whatever data object we are reflecting, and specify
    what that column should look like - as well as apply any aggregate functions we've
    defined.

    Note - You must explicitly add it to the Project object in order for it to run.

    Args:
        column_name: The name of the column in the data object - Generally either the column name
            in the pandas or SQL table.
        column_type: Useful if you want to enforce types in a pandas DataFrame. See (link here) DataTypes section
            for more information.
        force_type: If column_type is used, should the type be enforced. Setting this to True means that
            the entire process will halt if right type is not present.
        overwrite_type: This is useful if the aggregate function requires a specific datatype for it to be
            computed.
        null: Can the column contain missing values
        force_null: If null is set to False - should the process fail given there are missing values present.
        unique: Should uniqueness in the column be enforced.
        is_category: Denoting a column as a category has several consequences - including automatically
            collecting counts for each category.
        functions: A list of property defined functions.
        extra_functions: If this mapping is used for multiple columns but want a function to be applied to
            only one of the columns, use this. See example for more information.
    
    Returns:
        A column object that can be added to a Project. See Project for more details.

    """
    if functions is None:
        functions = []
    if extra_functions is None:
        extra_functions = {}

    def return_column_dict(name: str) -> Dict[str, Any]:
        dict_ = {
            "name": column_name,
            "type": column_type,
            "force_type": force_type,
            "overwrite_type": overwrite_type,
            "null": null,
            "force_null": force_null,
            "unique": unique,
            "is_category": is_category,
            "functions": _get_functions(functions, column_name=name),
            "extra_functions": _get_functions(extra_functions, column_name=name),
        }
        return dict_

    return_column_dict.column_name = column_name
    return return_column_dict
