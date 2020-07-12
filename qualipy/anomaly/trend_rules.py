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
    "different_from_mode": {
        "function": different_from_mode,
        "display_name": "Different From Mode",
        "description": """This will notify each time the value is different from the
                          mode. This is useful when monitoring trends that are usually constant""",
    },
    "mono_increasing": {
        "function": mono_increasing,
        "display_name": "Monotonic Increasing",
        "description": "This trend must never decrease. Staying constant is accepted",
    },
    "mono_decreasing": {
        "function": mono_decreasing,
        "display_name": "Monotonic Decreasing",
        "description": "This trend must never increase. Staying constant is accepted",
    },
    "increasing": {
        "function": increasing,
        "display_name": "Increasing",
        "description": "This trend must always increase. Staying constant is not accepted",
    },
}
