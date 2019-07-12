from functools import wraps

import types
from typing import Any, Dict, List, Callable, Optional, Union


def copy_func(f: Callable, name: Optional[str] = None) -> Callable:
    fn = types.FunctionType(
        f.__code__, f.__globals__, name or f.__name__, f.__defaults__, f.__closure__
    )
    fn.__dict__.update(f.__dict__)
    return fn


def copy_function_spec(function: Union[Dict[str, Any], Callable]):
    if isinstance(function, dict):
        copied_function = copy_func(function["function"])
        copied_function.arguments = function.get("parameters", {})
    else:
        copied_function = copy_func(function)
        copied_function.arguments = {}
    return copied_function


def function(
    allowed_arguments: Optional[List[str]] = None,
    return_format: type = float,
    arguments: Dict[str, Any] = None,
    anomaly: bool = False,
    other_column: Union[Optional[List[str]], Optional[str]] = None,
    fail: bool = False,
) -> Callable:
    def inner_fun(method: Callable):
        method.anomaly = anomaly
        method.allowed_arguments = (
            [] if allowed_arguments is None else allowed_arguments
        )
        method.arguments = {} if arguments is None else arguments
        method.has_decorator = True
        method.return_format = return_format
        method.other_column = other_column
        method.fail = fail

        @wraps(method)
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)

        return wrapper

    return inner_fun


class Column(object):

    column_name = None
    column_type = None
    force_type = False
    null = True
    force_null = False
    unique = False
    functions = []

    def _as_dict(self, name: str) -> Dict[str, Any]:
        dict_ = {
            "name": name,
            "type": self.column_type,
            "force_type": self.force_type,
            "null": self.null,
            "force_null": self.force_null,
            "unique": self.unique,
            "functions": self._get_functions(),
        }
        return dict_

    def _get_functions(self) -> Dict[str, Callable]:
        methods = {}
        for fun in dir(self):
            function = getattr(self, fun, None)
            if getattr(function, "has_decorator", False):
                methods[fun] = function
        given_methods = getattr(self, "functions", None)
        if given_methods:
            for func in given_methods:
                copied_function = copy_function_spec(func)
                methods[copied_function.__name__] = copied_function
        return methods
