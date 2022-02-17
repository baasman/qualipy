import logging
from typing import Any, Dict, List, Callable, Optional, Union
from functools import wraps
import importlib
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass
class FunctionTracker:
    name: str
    module: str


def function(
    allowed_arguments: Optional[List[str]] = None,
    return_format: type = Union[float, str],
    arguments: Dict[str, Any] = None,
    fail: bool = False,
    display_name: str = None,
    description: str = None,
    input_format: type = float,
    custom_value_return_format: type = None,
) -> Callable:
    """Define a function that can be applied to a qualipy dataset

    Use this decorator to specify a qualipy function, and describe
    how it will function when executed. Whatever function this decorator
    is used for must abide by three rules:

    1) The first argument must be `data` - This is the data object you pass to Qualipy
    2) The second argument is the column - This is the name of the column the function
        is being applied to.
    3) Any arguments as they correspond to `allowed_arguments` - They must contain the same
        name exactly.

    Args:
        allowed_arguments: An optional list that specifies what
            arguments can be passed to the function at runtime
        return_format: Used for rendering purposes on the reporting. Can be either
            float, int, str, dict, or bool
        fail: If this rule returns a boolean, should the process halt given
            a False?
        display_name: This is how the function would be displayed on a report.
            if not given, it will take the name of the function itself
        description: If given, this will be displayed when hovering over the function
            name in a report

    Returns:
        Any value that corresponds to the appropriate return_format

    """

    def inner_fun(method: Callable):
        method.allowed_arguments = (
            [] if allowed_arguments is None else allowed_arguments
        )
        method.arguments = {} if arguments is None else arguments
        method.has_decorator = True
        method.return_format = return_format
        method.custom_value_return_format = custom_value_return_format
        method.input_format = input_format
        method.fail = fail
        method.valid_min_range = None
        method.valid_max_range = None
        method.display_name = method.__name__ if display_name is None else display_name
        method.description = "" if description is None else description

        if return_format == "custom" and custom_value_return_format is None:
            raise ValueError(
                "If using custom format, must set the custom_value_return_format"
            )

        @wraps(method)
        def wrapper(*args, **kwargs):
            logger.info(f"Running method: {method.__name__}")
            return method(*args, **kwargs)

        return wrapper

    return inner_fun


def function_from_module(full_module_path, function_name):
    spec = importlib.import_module(full_module_path)
    function_obj = getattr(spec, function_name)
    return function_obj


if __name__ == "__main__":
    function_from_module("qualipy.backends.pandas_backend.functions", "mean")
