from qualipy.measure_util import (
    _get_mean,
    _get_std,
    _get_nunique
)


MEASURE_MAP = {
    'mean': _get_mean,
    'std': _get_std,
    'nunique': _get_nunique
}


def _generate_descriptions(column, measure):
    return {
        'value': MEASURE_MAP[measure](column),
        '_metric': measure
    }

