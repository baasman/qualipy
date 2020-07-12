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
