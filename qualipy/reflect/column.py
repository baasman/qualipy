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
