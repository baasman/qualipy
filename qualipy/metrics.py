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


def _generate_num_descriptions(column, measures):
    results = {}
    for measure in measures:
        results[measure] = MEASURE_MAP[measure](column)
    return results


def _generate_cat_descriptions(column, measures):
    results = {}
    for measure in measures:
        results[measure] = MEASURE_MAP[measure](column)
    return results
