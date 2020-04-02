from operator import eq

import numpy as np


class BaseType(object):
    def __repr__(self):
        class_name = str(self.__class__).split(".")[-1]
        class_name = class_name.replace("'>", "")
        return class_name


class DateTimeType(BaseType):

    str_name = "datetime64"

    def check_approximate_type(self, given_dtype):
        return eq(given_dtype.type, np.datetime64)


class FloatType(BaseType):

    str_name = "float64"

    def check_approximate_type(self, given_dtype):
        types = [
            eq(given_dtype, numpy_float_type)
            for numpy_float_type in [
                np.float,
                np.float16,
                np.float32,
                np.float64,
                np.float128,
            ]
        ]
        return any(types)


class IntType(BaseType):

    str_name = "int16"

    def check_approximate_type(self, given_dtype):
        types = [
            eq(given_dtype, numpy_float_type)
            for numpy_float_type in [
                np.int,
                np.int0,
                np.int8,
                np.int16,
                np.int32,
                np.int64,
            ]
        ]
        return any(types)


class ObjectType(BaseType):

    str_name = "str"

    def check_approximate_type(self, given_dtype):
        return eq(given_dtype, object)


class NumericType(BaseType):

    str_name = "float64"

    def check_approximate_type(self, given_dtype):
        return np.issubdtype(given_dtype, np.number)


class BoolType(BaseType):

    str_name = "bool"

    def check_approximate_type(self, given_dtype):
        return eq(given_dtype, bool)
