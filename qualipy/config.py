import os
import json
import typing as t
import logging
from functools import wraps

from qualipy.store.util import _initialize_tables, create_sqlalchemy_engine

logger = logging.getLogger(__name__)


DEFAULT_PROJECT_CONFIG = {
    "ANOMALY_ARGS": {
        "check_for_std": False,
        "importance_level": 1,
        "distance_from_bound": 1,
        "ignore": [],
        "specific": {},
    },
    "PROFILE_ARGS": {},
    "ANOMALY_MODEL": "std",
    "DATE_FORMAT": "%Y-%m-%d",
    "NUM_SEVERITY_LEVEL": 1,
    "CAT_SEVERITY_LEVEL": 1,
    "VISUALIZATION": {
        "row_counts": {
            "include_bar_of_latest": {
                "use": True,
                "diff": False,
                "show_by_default": True,
            },
            "include_summary": {"use": True, "show_by_default": True},
        },
        "trend": {
            "include_bar_of_latest": {
                "use": True,
                "diff": False,
                "show_by_default": True,
            },
            "include_summary": {"use": True, "show_by_default": True},
            "sst": 3,
            "point": True,
            "n_steps": 10,
            "add_diff": {"shift": 1},
        },
        "missing": {
            "include_0": True,
            "include_bar_of_latest": {"use": True, "diff": False},
        },
    },
}


def _set_default_config(config_dir, overwrite_kwargs=None):
    if "QUALIPY_DB" not in overwrite_kwargs:
        db_url = f'sqlite:///{os.path.join(config_dir, "qualipy.db")}'
        overwrite_kwargs["QUALIPY_DB"] = db_url
    return overwrite_kwargs


def _generate_config(config_dir, overwrite_kwargs: dict = None):
    overwrite_kwargs = {} if overwrite_kwargs is None else overwrite_kwargs
    os.makedirs(config_dir, exist_ok=True)
    with open(os.path.join(config_dir, "config.json"), "w") as f:
        json.dump(_set_default_config(config_dir, overwrite_kwargs), f)
    with open(os.path.join(config_dir, "projects.json"), "w") as f:
        json.dump({}, f)
    os.makedirs(os.path.join(config_dir, "models"), exist_ok=True)
    os.makedirs(os.path.join(config_dir, "profile_data"), exist_ok=True)
    os.makedirs(os.path.join(config_dir, "reports"), exist_ok=True)
    os.makedirs(os.path.join(config_dir, "reports", "anomaly"), exist_ok=True)
    os.makedirs(os.path.join(config_dir, "reports", "profiler"), exist_ok=True)
    os.makedirs(os.path.join(config_dir, "reports", "comparison"), exist_ok=True)


def generate_config(
    config_dir, create_in_empty_dir=False, overwrite_kwargs: dict = None
):
    config_dir = os.path.expanduser(config_dir)
    if not os.path.exists(config_dir):
        _generate_config(config_dir=config_dir, overwrite_kwargs=overwrite_kwargs)
    else:
        config_already_exists = os.path.exists(os.path.join(config_dir, "config.json"))
        if create_in_empty_dir and not config_already_exists:
            _generate_config(config_dir=config_dir, overwrite_kwargs=overwrite_kwargs)
        if overwrite_kwargs is not None:
            with open(os.path.join(config_dir, "config.json"), "r") as f:
                conf = json.load(f)
            conf = {**conf, **overwrite_kwargs}
            with open(os.path.join(config_dir, "config.json"), "w") as f:
                json.dump(conf, f)
        if not config_already_exists:
            raise Exception(
                "Error: Make sure directory follows proper Qualipy structure"
            )
    conf = QualipyConfig(config_dir=config_dir)
    return conf


class QualipyConfig(dict):
    def __init__(
        self, config_dir: str, project_name: str = None, defaults: dict = None
    ):
        dict.__init__(self, defaults or {})
        self.config_dir = config_dir
        self.project_name = project_name
        self._read_conf_from_file()
        _initialize_tables(self)

    def _read_conf_from_file(self):
        try:
            with open(os.path.join(self.config_dir, "config.json"), "r") as f:
                config = json.load(f)
        except FileNotFoundError:
            logger.info(f"Config not found. Autogeneration at {self.config_dir}")
            generate_config(config_dir=self.config_dir)
            with open(os.path.join(self.config_dir, "config.json"), "r") as f:
                config = json.load(f)
        self.from_mapping(config)
        return True

    def from_mapping(
        self, mapping: t.Optional[t.Mapping[str, t.Any]] = None, **kwargs: t.Any
    ) -> bool:
        mappings: t.Dict[str, t.Any] = {}
        if mapping is not None:
            mappings.update(mapping)
        mappings.update(kwargs)
        self.update(mappings)
        return True

    def set_default_project_config(self, project_name):
        if project_name not in self:
            self[project_name] = DEFAULT_PROJECT_CONFIG
        with open(os.path.join(self.config_dir, "config.json"), "w") as f:
            json.dump(self, f)

    def add_tracking_db(
        self,
        name: str,
        drivername: str,
        username: str,
        password: str,
        host: str,
        port: int,
        query: dict,
    ):
        self._read_conf_from_file()
        if "TRACKING_DBS" not in self:
            self["TRACKING_DBS"] = {}

        spec = dict(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
        )
        if query is not None:
            query_args = {}
            if isinstance(query, list):
                for inp in query:
                    query_args[inp[0]] = inp[1]
            elif isinstance(query, dict):
                for k, v in query.items():
                    query_args[k] = v
            spec["query"] = query_args
        self["TRACKING_DBS"][name] = spec
        self.dump()

    def add_spark_sql_conn(
        self,
        name: str,
        master: str,
        jdbc_url: str,
        username: str,
        password: str,
        drivername: int,
        pairs: t.List[t.Tuple],
    ):
        self._read_conf_from_file()
        if "SPARK_CONN" not in self:
            self["SPARK_CONN"] = {}

        spec = dict(
            driver=drivername,
            username=username,
            password=password,
            jdbc_url=jdbc_url,
            master=master,
            pairs=pairs,
        )
        self["SPARK_CONN"][name] = spec
        self.dump()

    def add_trend_rule(
        self,
        project_name: str,
        metric_id: str,
        trend_name: str,
        severity: int,
        arguments: t.Dict[str, t.Any],
    ):
        self._read_conf_from_file()
        if project_name not in self:
            raise Exception("Save project before adding trend rules")
        if "ANOMALY_ARGS" not in self[project_name]:
            self[project_name]["ANOMALY_ARGS"] = {
                "check_for_std": False,
                "importance_level": 1,
                "distance_from_bound": 1,
                "ignore": [],
                "specific": {},
            }
        trend_function = {
            "function": trend_name,
            "arguments": arguments,
            "severity": severity,
        }
        if metric_id in self[project_name]["ANOMALY_ARGS"]["specific"]:
            fun_hashes = [
                hash(str(i))
                for i in self[project_name]["ANOMALY_ARGS"]["specific"][metric_id]
            ]
            if hash(str(trend_function)) in fun_hashes:
                logger.info("Trend function already addedd")
            else:
                self[project_name]["ANOMALY_ARGS"]["specific"][metric_id].append(
                    trend_function
                )
        else:
            self[project_name]["ANOMALY_ARGS"]["specific"][metric_id] = [trend_function]
        self.dump()

    def get_projects(self):
        try:
            with open(os.path.join(self.config_dir, "projects.json"), "r") as f:
                projects = json.loads(f.read())
        except:
            projects = {}
        return projects

    def dump(self):
        with open(os.path.join(self.config_dir, "config.json"), "w") as f:
            json.dump(self, f)

    def validate_schema(self):
        pass
