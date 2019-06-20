from copy import deepcopy


class Project(object):

    def __init__(self, project_name, backend='pandas'):
        self.project_name = project_name
        self.backend = backend
        self.columns = {}

    def add_column(self, column):
        if isinstance(column, list):
            for col in column:
                self._add_column(col)
        else:
            self._add_column(column)

    def _add_column(self, column):
        if isinstance(column.column_name, list):
            for col in column.column_name:
                self.columns[col] = column._as_dict(col)
        else:
            self.columns[column.column_name] = column._as_dict(column.column_name)
