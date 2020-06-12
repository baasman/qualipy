from qualipy.anomaly._isolation_forest import IsolationForestModel
from qualipy.anomaly._prophet import ProphetModel
from qualipy.anomaly._std import STDCheck

import joblib

import json
import os


def create_file_name(model_dir, metric_id):
    file_name = os.path.join(model_dir, f"{metric_id}.mod")
    return file_name


class AnomalyModel(object):

    mods = {
        "IsolationForest": IsolationForestModel,
        "prophet": ProphetModel,
        "std": STDCheck,
    }

    def __init__(
        self, config_loc, metric_name, project_name=None, model=None, arguments=None,
    ):
        with open(os.path.join(config_loc, "config.json"), "r") as c:
            config = json.load(c)

        self.metric_name = metric_name
        if model is None and arguments is None:
            model = config[project_name].get("ANOMALY_MODEL", "std")
            arguments = config[project_name].get("ANOMALY_ARGS", {})
        self.anom_model = self.mods[model](self.metric_name, arguments)
        self.model_dir = os.path.join(config_loc, "models")
        if not os.path.isdir(self.model_dir):
            os.mkdir(self.model_dir)

    def train(self, train_data):
        self.anom_model.fit(train_data)

    def predict(self, test_data, check_for_std=False, multivariate=False):
        return self.anom_model.predict(
            test_data, check_for_std=check_for_std, multivariate=multivariate
        )

    def train_predict(self, train_data, **kwargs):
        return self.anom_model.train_predict(train_data, **kwargs)

    def save(self):
        file_name = create_file_name(self.model_dir, self.metric_name)
        joblib.dump(self.anom_model, file_name)


class LoadedModel(object):
    def __init__(self, config_loc):
        self.model_dir = os.path.join(config_loc, "models")

    def load(self, metric_id):
        file_name = create_file_name(self.model_dir, metric_id)
        self.anom_model = joblib.load(file_name)

    def predict(self, test_data, check_for_std=False, multivariate=False):
        return self.anom_model.predict(
            test_data, check_for_std=check_for_std, multivariate=multivariate
        )

    def train_predict(self, train_data):
        return self.anom_model.fit_predict(train_data)