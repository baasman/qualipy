try:
    from fbprophet import Prophet
except ImportError:
    print("fbprophet not installed. Can not be used for anomaly training")
import numpy as np

from qualipy.anomaly.base import AnomalyModelImplementation


class ProphetModel(AnomalyModelImplementation):
    def __init__(self, config_dir, metric_name, project_name=None, arguments=None):
        super(ProphetModel, self).__init__(
            config_dir, metric_name, project_name, arguments
        )
        self.check_for_std = self.arguments.pop("check_for_std", False)
        self.importance_level = self.arguments.pop("importance_level", 0)
        self.distance_from_bound = self.arguments.pop("distance_from_bound", 0)
        _model_specific_conf = self.arguments.pop("metric_specific_conf", {})
        self.model_specific_conf = _model_specific_conf.get(self.metric_name, {})

        self.importance_level = self.model_specific_conf.get(
            "importance_level", self.importance_level
        )
        self.distance_from_bound = self.model_specific_conf.get(
            "distance_from_bound", self.distance_from_bound
        )

        self.model = Prophet(**self.arguments)

    def fit(self, train_data):
        train_data = train_data[["date", "value"]].rename(
            columns={"date": "ds", "value": "y"}
        )
        train_data.ds = train_data.ds.dt.tz_localize(None)
        self.model.fit(train_data)

    def predict(self, test_data):
        test_data = test_data[["date", "value"]].rename(
            columns={"date": "ds", "value": "y"}
        )
        test_data.ds = test_data.ds.dt.tz_localize(None)
        predicted = self.model.predict(test_data)
        predicted = predicted.set_index(test_data.index)
        predicted["y"] = test_data["y"]

        predicted["outlier"] = 1
        predicted["outlier"] = np.where(predicted.y < predicted.yhat_lower - .0001, -1, 1)
        predicted["outlier"] = np.where(predicted.y > predicted.yhat_upper + .0001, -1, 1)

        predicted["importance"] = np.NaN
        predicted.loc[predicted.outlier == 1, "importance"] = (
            predicted.y - predicted.yhat_upper
        ) / (predicted.yhat_upper - predicted.yhat_lower)
        predicted.loc[predicted.outlier == -1, "importance"] = (
            predicted.yhat_lower - predicted.y
        ) / (predicted.yhat_upper - predicted.yhat_lower)
        predicted.importance = predicted.importance.abs()
        predicted["score"] = (predicted["y"] - predicted["yhat_upper"]) * (
            predicted["y"] >= predicted["yhat"]
        ) + (predicted["yhat_lower"] - predicted["y"]) * (
            predicted["y"] < predicted["yhat"]
        )
        predicted["standardized_score"] = (
            (predicted.score - predicted.score.mean()) / predicted.score.std()
        ).abs()
        if self.check_for_std:
            std = test_data.y.std(ddof=0)
            mean = test_data.y.mean()
            zscores = (test_data.y - mean) / std
            std_outliers = (zscores < -3) | (zscores > 3)
            predicted["zscore_outlier"] = std_outliers
            predicted["zscore"] = zscores
            predicted["outlier"] = np.where(
                (predicted.outlier == -1)
                & (predicted.standardized_score > self.distance_from_bound)
                & (predicted.importance > self.importance_level)
                & ((predicted.y < 3) | (predicted.y > 3)),
                -1,
                1,
            )
        else:
            predicted["outlier"] = np.where(
                (predicted.outlier == -1)
                & (predicted.standardized_score > self.distance_from_bound)
                & (predicted.importance > self.importance_level),
                -1,
                1,
            )

        if predicted[predicted.outlier == -1].shape[0] > 0:
            print(predicted)
        predicted["distance_from_expected"] = predicted.y - predicted.yhat
        return predicted.outlier.values, predicted.importance.values

    def train_predict(self, train_data):
        train_data = train_data[["date", "value"]].rename(
            columns={"date": "ds", "value": "y"}
        )
        predicted = self.model.fit(train_data).predict(train_data)
        predicted = predicted.set_index(train_data.index)
        predicted["y"] = train_data["y"]
        predicted["outlier"] = np.where(
            (predicted.y > predicted.yhat_upper) | (predicted.y < predicted.yhat_lower),
            -1,
            1,
        )
        return predicted.outlier
