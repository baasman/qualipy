import abc
import os
import pickle

import sqlalchemy as sa

from qualipy.store.initial_models import AnomalyModel, Value
from qualipy.exceptions import ModelNotFound


class AnomalyModelImplementation(abc.ABC):
    """
    Add docstring here.
    Note: must produce class with attribute 'model'
    Note: self.arguments contains the arguments used to configure the
          anomaly model
    """

    def __init__(
        self,
        project,
        metric_name,
        value_ids,
        project_name=None,
        arguments=None,
    ):
        self.project = project
        self.config = project.config
        self.value_ids = value_ids
        self.metric_name = metric_name
        if arguments is None:
            arguments = self.config[project_name].get("ANOMALY_ARGS", {})
        self.specific = arguments.pop("specific", {})
        self.arguments = arguments
        self.model_dir = os.path.join(self.config.config_dir, "models")
        if not os.path.isdir(self.model_dir):
            os.mkdir(self.model_dir)

    @abc.abstractmethod
    def fit(self, train_data):
        return

    @abc.abstractmethod
    def predict(self, test_data):
        return

    def save(self):
        return


class LoadedModel:
    def __init__(self, project):
        self.project = project
        self.anom_model = None

    def load(self, data_row):
        existing_metric_value = (
            self.project.session.query(Value)
            .filter(
                sa.and_(
                    Value.project_id == str(data_row["project_id"]),
                    Value.metric == data_row["metric"],
                    Value.column_name == data_row["original_column_name"],
                    Value.anomaly_model_id != None,
                )
            )
            .first()
        )
        if existing_metric_value is None:
            raise ModelNotFound("Unable to find model")
        if existing_metric_value.anomaly_model is None:
            raise ModelNotFound("Unable to find model")
        self.anom_model = pickle.loads(existing_metric_value.anomaly_model.model_blob)

    def predict(self, test_data):
        return self.anom_model.predict(test_data)
