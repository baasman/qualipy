from functools import wraps


def function(anomaly=False):
    print(anomaly)
    def inner_fun(method):
        @wraps(method)
        def wrapper(*args, **kwargs):
            print(4)
            return method(*args, *kwargs)
        return wrapper

    return inner_fun


class Column(object):

    def __init__(self):
        assert hasattr(self, 'column_name')

    def _as_dict(self):
        pass

    def _get_methods(self):
        functions = [fun for fun in dir(self) if 'function' in fun]
        functions = {fun: getattr(self, fun) for fun in functions}
        return functions

    def __repr__(self):
        return '<(Column={})>'.format(self.column_name)


class Project(object):

    def __init__(self, project_name):
        self.project_name = project_name
        self.columns = []

    def add_column(self, column):
        if isinstance(column, list):
            for col in column:
                self._add_column(col)
        else:
            self._add_column(column)

    def _add_column(self, column):
        self.columns.append(column)

if __name__ == '__main__':
    import numpy as np
    from scipy import stats
    import pandas as pd

    df = pd.DataFrame({'score': [.5, .5, .6]})

    class MyCol(Column):

        column_name = 'score'
        column_type = 'float64'
        unique = False
        default_funs = ['mean', 'std']

        @function(anomaly=True)
        def function_greater_than_threshold(self, data, threshold):
            return data[data[self.column_name] > threshold].shape[0]

        def function_number_of_outliers(self, data, std_away):
            data = data[data[self.column_name].notnull()]
            return data[np.abs(stats.zscore(data[self.column_name])) > std_away].shape[0]

    score = MyCol()
    score.function_greater_than_threshold(df, .5)
