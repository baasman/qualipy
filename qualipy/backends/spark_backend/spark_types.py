from qualipy.backends.base import BaseType


class FloatType(BaseType):

    str_name = "float"

    def check_approximate_type(self, given_dtype):
        pass


class DoubleType(BaseType):

    str_name = "double"

    def check_approximate_type(self, given_dtype):
        pass


class StringType(BaseType):

    str_name = "string"

    def check_approximate_type(self, given_dtype):
        pass