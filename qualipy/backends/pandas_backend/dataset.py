from qualipy.backends.base import BaseData

from typing import List

import pandas as pd


class PandasData(BaseData):
    """PandasData must be instantiated when tracking pandas data"""

    def __init__(self, data: pd.DataFrame):
        """
        Args:
            data: The pandas dataset that we want to track
        """
        self.data = data
        # should make this private
        self.stratify = False

    def set_fallback_data(self):
        return self.data.head(0)

    def get_data(self, backend_used="pandas"):
        return self.data

    def set_stratify_rule(self, column: str, values: List[str] = None) -> None:
        """Use this when you want to run all functions on separate stratifications

        Currently, only equality based stratification is possible. In the future, comparison
        based stratifications will be available.

        Args:
            column: The name of the column you want to stratify on.
            values: If you only want to include a subset of values within ``column``,
                specify them here

        Returns:
            None
        """
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
