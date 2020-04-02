import os
import json
import uuid
from typing import Dict

from qualipy.project import create_qualipy_folder


basedir = os.path.abspath(os.path.dirname(__file__))

MUST_BE_PRESENT = []


def get_default_qualipy_db(config_dir):
    path = os.path.join(config_dir, "qualipy.db")
    return f"sqlite:///{path}"


def get_default_web_db(config_dir):
    path = os.path.join(config_dir, "app.db")
    return f"sqlite:///{path}"


def get_default_migrations_dir(config_dir):
    return os.path.join(config_dir, "migrations")


def check_for_missing(config: Dict):
    for key in MUST_BE_PRESENT:
        if key not in config:
            raise ValueError(f"Key: {key} must be present in the config file")


def read_project_config(config_dir):
    if not os.path.isdir(config_dir):
        create_qualipy_folder(config_dir, get_default_qualipy_db(config_dir))
    with open(os.path.join(config_dir, "config.json"), "r") as f:
        _config = json.load(f)
    check_for_missing(_config)
    return _config


class BaseConfig:
    def __init__(self, config_dir):
        self.SECRET_KEY = str(uuid.uuid4())
        self.INTERVAL_TIME = 100000
        self.DEBUG = False
        self.REMEMBER_COOKIE_DURATION = 365
        self.QUALIPY_DB = get_default_qualipy_db(config_dir)
        self.SQLALCHEMY_DATABASE_URI = get_default_web_db(config_dir)
        self.MIGRATIONS_DIR = get_default_migrations_dir(config_dir)
        self.SQLALCHEMY_TRACK_MODIFICATIONS = False
        self.DB_AUTO_CREATE = True
        self.DB_AUTO_UPGRADE = True
        self.USERS = {"admin": "admin"}
        self.CACHE_REDIS_URL = os.getenv("CACHE_REDIS_URL", "redis://localhost:6379")
