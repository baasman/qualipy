from flask_caching import Cache
import pyarrow as pa


cache = Cache(config={"CACHE_TYPE": "simple"})


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
