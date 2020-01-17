from qualipy.web.app.metric_tracker.components.overview_page import (
    overview_table,
    schema_table,
    anomaly_num_data,
    anomaly_num_table,
)
from qualipy.anomaly_detection import anomaly_data_all_projects, anomaly_data_project

from flask_caching import Cache
from flask_login import current_user
from flask import session, current_app as capp
import pyarrow as pa


cache = Cache(config={"CACHE_TYPE": "redis", "CACHE_DEFAULT_TIMEOUT": 100000})


context = pa.default_serialization_context()


def set_session_data_name(session_id):
    return f"{session_id}-data"


def set_session_anom_data_name(session_id):
    return f"{session_id}-anom-data"


def cache_dataframe(dataframe, name):
    cache.set(name, context.serialize(dataframe).to_buffer().to_pybytes())


def get_cached_dataframe(name):
    obj = cache.get(name)
    if obj is None:
        return obj
    return context.deserialize(obj)


def get_and_cache_anom_table(all_projects=False):
    anom_data_session = set_session_anom_data_name(current_user.username)
    anom_data = get_cached_dataframe(anom_data_session)
    if anom_data is None:
        anom_data = anomaly_data_all_projects(
            project_names=session["all_projects"],
            db_url=capp.config["QUALIPY_DB"],
            config_dir=capp.config["CONFIG_DIR"],
        )
        cache_dataframe(anom_data, anom_data_session)
    if not all_projects:
        anom_data = anom_data[anom_data.project == session["project_name"]]
    return anom_data
