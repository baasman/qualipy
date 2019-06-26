from functools import wraps

import types

def copy_func(f, name=None):
    fn = types.FunctionType(f.__code__, f.__globals__, name or f.__name__,
        f.__defaults__, f.__closure__)
    fn.__dict__.update(f.__dict__)
    return fn


# TODO: should have separate decorators for methods and functions
def function(allowed_arguments=None, return_format=float,
             arguments=None, anomaly=False, other_column=None,
             fail=False):
    def inner_fun(method):
        method.anomaly = anomaly
        method.allowed_arguments = [] if allowed_arguments is None else allowed_arguments
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

    def _as_dict(self, name):
        dict_ = {
            'name': name,
            'type': getattr(self, 'column_type', None),
            'null': getattr(self, 'null', True),
            'unique': getattr(self, 'unique', True),
            'default_functions': self._get_default_methods(),
            'custom_functions': self._get_methods()
        }
        return dict_

    def _get_methods(self):
        methods = {}
        for fun in dir(self):
            function = getattr(self, fun, None)
            if getattr(function, 'has_decorator', False):
                methods[fun] = function
        custom_funcs = getattr(self, 'custom_funs', None)
        if custom_funcs:
            for custom_func in self.custom_funs:
                copied_function = self._copy_function_spec(custom_func)
                methods[copied_function.__name__] = copied_function
        return methods

    def _get_default_methods(self):
        default_functions = {}
        for function in getattr(self, 'default_funs', []):
            copied_function = self._copy_function_spec(function)
            default_functions[copied_function.__name__] = copied_function
        return default_functions

    def _copy_function_spec(self, function):
        if isinstance(function, dict):
            copied_function = copy_func(function['function'])
            copied_function.arguments = function.get('parameters', {})
        else:
            copied_function = copy_func(function)
            copied_function.arguments = {}
        return copied_function
