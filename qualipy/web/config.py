import os
import json
import uuid

basedir = os.path.abspath(os.path.dirname(__file__))


def read_project_config(config_dir):
    with open(os.path.join(config_dir, "config.json"), "r") as f:
        _config = json.load(f)
    return _config


class BaseConfig:
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = str(uuid.uuid4())
