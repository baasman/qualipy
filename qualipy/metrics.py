from qualipy.backends._pandas.metrics import (
    _get_std,
    _get_quantile,
    _get_min,
    _get_nunique,
    _get_top,
    _get_freq,
    _get_correlation,
    _get_number_of_outliers,
)


PANDAS_METRIC_MAP = {
    'std': _get_std,
    'quantile': _get_quantile,
    'min': _get_min,
    'nunique': _get_nunique,
    'top': _get_top,
    'freq': _get_freq,
    'correlation': _get_correlation,
    'number_of_outliers': _get_number_of_outliers,
}
