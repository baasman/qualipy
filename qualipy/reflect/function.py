from typing import Any, Dict, List, Callable, Optional
from functools import wraps


def function(
    allowed_arguments: Optional[List[str]] = None,
    return_format: type = float,
    arguments: Dict[str, Any] = None,
    fail: bool = False,
    display_name: str = None,
    description: str = None,
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
        method.fail = fail
        method.valid_min_range = None
        method.valid_max_range = None
        method.display_name = method.__name__ if display_name is None else display_name
        method.description = "" if description is None else description

        @wraps(method)
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)

        return wrapper

    return inner_fun
