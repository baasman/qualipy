import os
import json
import uuid
from typing import Dict


basedir = os.path.abspath(os.path.dirname(__file__))

MUST_BE_PRESENT = []


def get_default_qualipy_db():
    path = os.path.join(os.environ["CONFIG_DIR"], "qualipy.db")
    return f"sqlite:///{path}"


def get_default_web_db():
    path = os.path.join(os.environ["CONFIG_DIR"], "app.db")
    return f"sqlite:///{path}"


def get_default_migrations_dir():
    return os.path.join(os.environ["CONFIG_DIR"], "migrations")


def check_for_missing(config: Dict):
    for key in MUST_BE_PRESENT:
        if key not in config:
            raise ValueError(f"Key: {key} must be present in the config file")


def read_project_config(config_dir):
    with open(os.path.join(config_dir, "config.json"), "r") as f:
        _config = json.load(f)
    check_for_missing(_config)
    return _config


class BaseConfig:
    SECRET_KEY = str(uuid.uuid4())
    INTERVAL_TIME = 100000
    DEBUG = False
    REMEMBER_COOKIE_DURATION = 365
    QUALIPY_DB = get_default_qualipy_db()
    SQLALCHEMY_DATABASE_URI = get_default_web_db()
    MIGRATIONS_DIR = get_default_migrations_dir()
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    DB_AUTO_CREATE = True
    DB_AUTO_UPGRADE = True
    USERS = {"admin": "admin"}
