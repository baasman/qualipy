from qualipy.backends.base import BaseData

from typing import List

import pandas as pd


class PandasData(BaseData):
    def __init__(self, data: pd.DataFrame, config=None, stratify: bool = False):
        self.data = data
        self.stratify = stratify

    def set_fallback_data(self):
        return self.data.head(0)

    def set_stratify_rule(self, column: str, values: List = None):
        self.stratify = True
        self.column = column
        if values is None:
            values = self.data[column].unique().tolist()
        if not isinstance(values, list):
            values = [values]
        self.stratify_values = values

    def subset_function(self):
        def _subset_function(data, value):
            _data = data[data[self.column] == value]
            return _data

        return _subset_function
