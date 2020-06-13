from sklearn.ensemble import IsolationForest
from scipy.stats import zscore
import pandas as pd
import numpy as np

from qualipy.anomaly.base import AnomalyModelImplementation


class IsolationForestModel(AnomalyModelImplementation):
    def __init__(self, metric_name, kwargs):
        self.multivariate = kwargs.pop("multivariate", True)
        self.check_for_std = kwargs.pop("check_for_std", True)
        defaults = {"contamination": 0.05}
        kwargs = {**kwargs, **defaults}
        self.model = IsolationForest(**kwargs)

    def fit(self, train_data):
        self.model.fit(train_data.value.values.reshape((-1, 1)))

    def predict(self, test_data):
        if self.multivariate:
            preds = self.model.predict(test_data)
        else:
            preds = self.model.predict(test_data.value.values.reshape((-1, 1)))

        if self.check_for_std and not self.multivariate:
            std = test_data.value.std()
            mean = test_data.value.mean()
            std_outliers = (test_data.value < mean - (3 * std)) | (
                test_data.value > mean + (3 * std)
            )
            preds = [
                -1 if mod_val == -1 and std else 1
                for mod_val, std in zip(preds, std_outliers)
            ]
        return np.array(preds)

    def train_predict(self, train_data, **kwargs):
        if isinstance(train_data, pd.DataFrame):
            self.model.fit(train_data)
            return self.predict(train_data, **kwargs)
        else:
            self.model.fit(train_data.value.values.reshape((-1, 1)))
            return self.predict(train_data.value.values.reshape((-1, 1)))
