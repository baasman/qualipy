import numpy as np
import pandas as pd
from scipy.stats import zscore

from qualipy.anomaly.base import AnomalyModelImplementation


class STDCheck(AnomalyModelImplementation):
    def __init__(
        self,
        project,
        metric_name,
        value_ids,
        project_name=None,
        arguments=None,
    ):
        super(STDCheck, self).__init__(
            project, metric_name, value_ids, project_name, arguments
        )
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
            preds = []
            final_zscores = []
            for idx in range(test_data.shape[0]):
                values = test_data.value[: idx + 1]
                try:
                    zscores = np.array(zscore(values))
                    std_outliers = (zscores < -self.std) | (zscores > self.std)
                    final_zscores.append(zscores[-1])
                    if std_outliers[-1]:
                        preds.append(-1)
                    else:
                        preds.append(1)
                except IndexError:
                    preds.append(1)
                    final_zscores.append(np.NaN)
        return np.array(preds), np.array(final_zscores)
