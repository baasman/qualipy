import numpy as np
import pandas as pd
from scipy.stats import zscore


class STDCheck(object):
    def __init__(self, metric_name, kwargs):
        defaults = {"std": 4}
        kwargs = {**defaults, **kwargs}
        for k, v in kwargs.items():
            setattr(self, k, v)

    def fit(self, train_data):
        self.model = None

    def predict(self, test_data, check_for_std=False, multivariate=False):
        metric_name = test_data.metric.iloc[0]
        if multivariate:
            raise NotImplementedError
        else:
            std = test_data.value.std()
            mean = test_data.value.mean()

            # its useless to check for result set if count is really low
            # fix hardcoding of metric name here
            if (metric_name in ["count", "number_of_rows_per_patient"]) and std < 5:
                std_outliers = np.repeat(False, test_data.shape[0])
                zscores = np.repeat(0, test_data.shape[0])
            else:
                low = mean - (self.std * std)
                high = mean + (self.std * std)
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
