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


def consecutive_identical_values(data):
    data["value_diff"] = data.value.diff()
    consec_groups = data[data["value_diff"] == 0].groupby(
        (data["value_diff"] != 0).cumsum()
    )
    idx = []
    for _, group in consec_groups:
        if group.shape[0] > 5:
            idx.append(group.index[-1])
    outliers = data.loc[idx]
    return outliers


def sustained_strong_diff(data):
    orig_data = data.copy()
    data = data.reset_index()
    value_mean = data.value.mean()
    data["value_diff"] = data.value.diff().abs()
    split_idx = data[data.value_diff == data.value_diff.max()].index[0]
    split_1 = data.iloc[:split_idx]
    split_2 = data.iloc[split_idx + 1 :]
    if split_1.shape[0] > 50 and split_2.shape[0] > 50:
        shape_1 = split_1.shape[0]
        mean_1 = split_1.value.mean()
        perc_1 = (
            split_1[split_1.value > value_mean].shape[0]
            if mean_1 > value_mean
            else split_1[split_1.value < value_mean].shape[0]
        ) / shape_1
        shape_2 = split_2.shape[0]
        mean_2 = split_2.value.mean()
        perc_2 = (
            split_2[split_2.value > value_mean].shape[0]
            if mean_2 > value_mean
            else split_2[split_2.value < value_mean].shape[0]
        ) / shape_2
        if perc_1 > 0.9 and perc_2 > 0.9:
            return data.iloc[[split_idx]]
    return data.head(0)


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
    "consecutive_identical_values": {
        "function": consecutive_identical_values,
        "display_name": "Consecutive Identical Values",
        "description": "This trend identifies spans of time where the value of a trend does not change",
    },
    "sustained_strong_diff": {
        "function": sustained_strong_diff,
        "display_name": "Sustained Strong Difference",
        "description": "",
    },
}
