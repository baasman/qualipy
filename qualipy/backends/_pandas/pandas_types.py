from operator import eq
import re

import numpy as np


class BaseType(object):

    def __repr__(self):
        class_name = str(self.__class__).split('.')[-1]
        class_name = class_name.replace("\'>", '')
        return class_name


class DateTimeType(BaseType):

    def check_approximate_type(self, given_dtype):
        return eq(given_dtype.type, np.datetime64)


class FloatType(BaseType):

    def check_approximate_type(self, given_dtype):
        types = [eq(given_dtype, numpy_float_type) for numpy_float_type in
                 [np.float, np.float16, np.float32, np.float64, np.float128]]
        return any(types)


class IntType(BaseType):

    def check_approximate_type(self, given_dtype):
        types = [eq(given_dtype, numpy_float_type) for numpy_float_type in
                 [np.int, np.int0, np.int8, np.int16, np.int32, np.int64]]
        return any(types)


class ObjectType(BaseType):

    def check_approximate_type(self, given_dtype):
        return eq(given_dtype, object)
