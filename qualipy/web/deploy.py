import abc
import sys

from qualipy.web.app import create_app

from gunicorn.app.base import BaseApplication


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
        self.app.run(
            debug=self.app.config["DEBUG"], port=self.port, host=self.host, **kwargs
        )


class GUnicornDeploy(BaseApplication, QualipyDeployer):
    def __init__(self, *args, **kwargs):
        QualipyDeployer.__init__(self, *args, **kwargs)
        BaseApplication.__init__(self)

    def load_config(self):
        options = {"bind": f"{self.host}:{self.port}", "workers": self.workers}
        for k, v in options.items():
            self.cfg.set(k.lower(), v)

    def load(self):
        return self.app
