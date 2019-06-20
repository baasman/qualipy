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
    # fail: if returns false, stop program

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
                function = copy_func(custom_func['function'])
                function.arguments = custom_func.get('parameters', {})
                methods[function.__name__] = function
        return methods

    def _get_default_methods(self):
        default_functions = {}
        for fun in getattr(self, 'default_funs', []):
            default_functions[fun.__name__] = fun
        return default_functions
