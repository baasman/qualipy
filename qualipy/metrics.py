from qualipy.measure_util import (
    _get_mean,
    _get_std,
    _get_quantile,
    _get_min,
    _get_nunique
)


MEASURE_MAP = {
    'mean': _get_mean,
    'std': _get_std,
    'quantile': _get_quantile,
    'min': _get_min,
    'nunique': _get_nunique,
}
