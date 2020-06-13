import numpy as np
import pandas as pd
from scipy.stats import zscore

from qualipy.anomaly.base import AnomalyModelImplementation


class STDCheck(AnomalyModelImplementation):
    def __init__(self, metric_name, kwargs):
        self.multivariate = kwargs.pop("multivariate", True)
        defaults = {"std": 4}
        kwargs = {**defaults, **kwargs}
        for k, v in kwargs.items():
            setattr(self, k, v)

    def fit(self, train_data):
        self.model = None

    def predict(self, test_data):
        if self.multivariate:
            raise NotImplementedError
        else:
            # note: this doesnt work when theres missing data
            # should probably compute manually
            zscores = zscore(test_data.value)
            std_outliers = (zscores < -self.std) | (zscores > self.std)
            preds = [-1 if std == True else 1 for std in std_outliers]

        return np.array(preds), zscores

    def train_predict(self, train_data, **kwargs):
        if isinstance(train_data, pd.DataFrame):
            self.model.fit(train_data)
            return self.predict(train_data, **kwargs)
        else:
            self.model.fit(train_data.value.values.reshape((-1, 1)))
            return self.predict(train_data.value.values.reshape((-1, 1)))
