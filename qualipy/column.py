from functools import wraps

import types
from typing import (
    Any,
    Dict,
    Callable
)




def copy_func(f, name=None):
    fn = types.FunctionType(
        f.__code__, f.__globals__, name or f.__name__, f.__defaults__, f.__closure__
    )
    fn.__dict__.update(f.__dict__)
    return fn


def copy_function_spec(function):
    if isinstance(function, dict):
        copied_function = copy_func(function["function"])
        copied_function.arguments = function.get("parameters", {})
    else:
        copied_function = copy_func(function)
        copied_function.arguments = {}
    return copied_function


def function(
    allowed_arguments=None,
    return_format=float,
    arguments=None,
    anomaly=False,
    other_column=None,
    fail=False,
):
    def inner_fun(method):
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
    def _as_dict(self, name: str) -> Dict[str, Any]:
        dict_ = {
            "name": name,
            "type": getattr(self, "column_type", None),
            "force_type": getattr(self, "force_type", False),
            "null": getattr(self, "null", True),
            "force_null": getattr(self, "null", True),
            "unique": getattr(self, "unique", False),
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
