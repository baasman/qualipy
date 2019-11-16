import abc
import sys

from qualipy.web.app import create_app


class QualipyDeployer(abc.ABC):
    def __init__(
        self, config_dir, host="127.0.0.1", port=5007, workers=4, train_anomaly=False
    ):
        self.config_dir = config_dir
        self.host = host
        self.port = port
        self.workers = workers
        self.train_anomaly = train_anomaly
        self.app = create_app()

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError


class FlaskDeploy(QualipyDeployer):
    def run(self, **kwargs):
        return self.app.run(
            debug=self.app.config["DEBUG"], port=self.port, host=self.host, **kwargs
        )
