from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np

from qualipy.anomaly.base import AnomalyModelImplementation


class IsolationForestModel(AnomalyModelImplementation):
    def __init__(self, config_dir, metric_name, project_name=None, arguments=None):
        super(IsolationForestModel, self).__init__(
            config_dir, metric_name, project_name, arguments
        )
        self.multivariate = self.arguments.pop("multivariate", False)
        self.check_for_std = self.arguments.pop("check_for_std", False)
        defaults = {"contamination": 0.05}
        self.arguments = {**self.arguments, **defaults}
        self.model = IsolationForest(**self.arguments)

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

    def train_predict(self, train_data):
        if isinstance(train_data, pd.DataFrame):
            self.model.fit(train_data)
            return self.predict(train_data)
        else:
            self.model.fit(train_data.value.values.reshape((-1, 1)))
            return self.predict(train_data.value.values.reshape((-1, 1)))
