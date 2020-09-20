import numpy as np
import pandas as pd
from scipy.stats import zscore

from qualipy.anomaly.base import AnomalyModelImplementation


class STDCheck(AnomalyModelImplementation):
    def __init__(
        self, config_dir, metric_name, project_name=None, arguments=None,
    ):
        super(STDCheck, self).__init__(config_dir, metric_name, project_name, arguments)
        self.multivariate = self.arguments.pop("multivariate", False)
        self.std = self.arguments.pop("std", 4)

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
