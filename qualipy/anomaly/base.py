import abc


class AnomalyModelImplementation(abc.ABC):
    @abc.abstractmethod
    def __init__(self, metric_name, kwargs):
        pass

    @abc.abstractmethod
    def fit(self, train_data):
        pass

    @abc.abstractmethod
    def predict(self, test_data):
        pass
