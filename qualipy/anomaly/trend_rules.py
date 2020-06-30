def mono_increasing(data):
    value_diff = data.value.diff()
    data = data[value_diff < 0]
    return data


def mono_decreasing(data):
    value_diff = data.value.diff()
    data = data[value_diff > 0]
    return data


def different_from_mode(data):
    mode = data.value.mode().iloc[0]
    data = data[data.value != mode]
    return data


def increasing(data):
    value_diff = data.value.diff()
    data = data[value_diff <= 0]
    return data


trend_rules = {
    "different_from_mode": different_from_mode,
    "mono_increasing": mono_increasing,
    "mono_decreasing": mono_decreasing,
    "increasing": increasing,
}
