from backends._pandas.metrics import (
    _get_mean,
    _get_std,
    _get_quantile,
    _get_min,
    _get_nunique,
    _get_top,
    _get_freq,
    _get_value_count,
    _get_perc_missing
)
from backends._spark.metrics import (
    _get_mean as _spark_get_mean,
    _get_nunique as _spark_get_nunique,
    _get_std as _spark_get_std
)


PANDAS_METRIC_MAP = {
    'mean': _get_mean,
    'std': _get_std,
    'quantile': _get_quantile,
    'min': _get_min,
    'nunique': _get_nunique,
    'top': _get_top,
    'freq': _get_freq,
    'value_counts': _get_value_count,
    'perc_missing': _get_perc_missing
}


SPARK_METRIC_MAP = {
    'mean': _spark_get_mean,
    'std': _spark_get_std,
    'nunique': _spark_get_nunique,
}
