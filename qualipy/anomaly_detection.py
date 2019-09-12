from sklearn.ensemble import IsolationForest
import joblib

import os


mods = {"IsolationForest": IsolationForest}
default_config_path = os.path.join(os.path.expanduser("~"), ".qualipy")


def create_file_name(model_dir, project_name, col_name, metric_name, arguments):
    file_name = os.path.join(
        model_dir, f"{project_name}_{col_name}_{metric_name}_{arguments}"
    )
    return file_name


class AnomalyModel(object):
    def __init__(
        self, model="IsolationForest", args=None, config_loc=default_config_path
    ):
        self.args = {} if args is None else args
        self.anom_model = mods[model](**args)
        self.model_dir = os.path.join(config_loc, "models")
        if not os.path.isdir(self.model_dir):
            os.mkdir(self.model_dir)

    def train(self, train_data):
        self.anom_model.fit(train_data)

    def save(self, project_name, col_name, metric_name, arguments=None):
        file_name = create_file_name(
            self.model_dir, project_name, col_name, metric_name, arguments
        )
        joblib.dump(self.anom_model, file_name)


class LoadedModel:
    def __init__(self, config_loc=default_config_path):
        self.model_dir = os.path.join(config_loc, "models")

    def load(self, project_name, col_name, metric_name, arguments=None):
        file_name = create_file_name(
            self.model_dir, project_name, col_name, metric_name, arguments
        )
        self.anom_model = joblib.load(file_name)

    def predict(self, test_data):
        return self.anom_model.predict(test_data)
