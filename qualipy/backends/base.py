import abc


class BackendBase(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def set_schema(data, columns):
        return

    @staticmethod
    @abc.abstractmethod
    def generate_description(function, data, column,
                             date, function_name, standard_viz,
                             over_time=True, other_columns=None, kwargs=None):
        return

    @staticmethod
    @abc.abstractmethod
    def set_return_value_type(value, return_format):
        return

    @staticmethod
    @abc.abstractmethod
    def set_column_type(data, column, type):
        return

    @staticmethod
    @abc.abstractmethod
    def get_other_columns(other_column, arguments, data):
        return

    @staticmethod
    @abc.abstractmethod
    def get_dtype(data, column):
        return

