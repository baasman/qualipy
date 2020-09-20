import abc
import json
import os

import joblib
import jinja2


def create_file_name(model_dir, metric_id):
    file_name = os.path.join(model_dir, f"{metric_id}.mod")
    return file_name


class AnomalyModelImplementation(abc.ABC):
    """
    Add docstring here.
    Note: must produce class with attribute 'model'
    Note: self.arguments contains the arguments used to configure the
          anomaly model
    """

    def __init__(
        self, config_dir, metric_name, project_name=None, arguments=None,
    ):
        with open(os.path.join(config_dir, "config.json"), "r") as conf_file:
            config = json.load(conf_file)

        self.metric_name = metric_name
        if arguments is None:
            arguments = config[project_name].get("ANOMALY_ARGS", {})
        self.specific = arguments.pop("specific", {})
        self.arguments = arguments
        self.model_dir = os.path.join(config_dir, "models")
        if not os.path.isdir(self.model_dir):
            os.mkdir(self.model_dir)

    @abc.abstractmethod
    def fit(self, train_data):
        return

    @abc.abstractmethod
    def predict(self, test_data):
        return

    def save(self):
        file_name = create_file_name(self.model_dir, self.metric_name)
        joblib.dump(self, file_name)


class LoadedModel:
    def __init__(self, config_dir):
        self.model_dir = os.path.join(config_dir, "models")
        self.anom_model = None

    def load(self, metric_id):
        file_name = create_file_name(self.model_dir, metric_id)
        self.anom_model = joblib.load(file_name)

    def predict(self, test_data):
        return self.anom_model.predict(test_data)
