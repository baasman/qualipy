import copy

import numpy as np
import pandas as pd
from scipy.stats import zscore
import pickle

from qualipy.anomaly.base import AnomalyModelImplementation
from qualipy.store.initial_models import Value, AnomalyModel


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

    def save(self):
        saveble_object = copy.copy(self)
        saveble_object.project = None
        model = AnomalyModel(model_blob=pickle.dumps(saveble_object), model_type="std")
        values = (
            self.project.session.query(Value)
            .filter(Value.value_id.in_(self.value_ids))
            .all()
        )
        for value in values:
            model.values.append(value)
        self.project.session.add(model)
        self.project.session.commit()

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
