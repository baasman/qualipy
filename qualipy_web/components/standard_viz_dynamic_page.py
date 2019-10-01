from functools import reduce

import dash_core_components as dcc
import numpy as np
import pandas as pd


def running_mean(x, N):
    cumsum = np.cumsum(np.insert(x, 0, 0))
    return (cumsum[N:] - cumsum[:-N]) / float(N)


def create_value_count_area_chart(data, var):
    data_values = data["value"].tolist()
    traces = []
    unique_vals = reduce(lambda x, y: x.union(y), [set(i.keys()) for i in data_values])
    x = data["date"]
    for value in unique_vals:
        traces.append(
            dict(
                x=data["date"],
                y=[i.get(value, 0) for i in data_values],
                hoverinfo="x+y",
                mode="lines",
                line=dict(width=0.5),
                stackgroup="one",
                name=value,
            )
        )

    plot = dcc.Graph(
        id="value-count-graph-{}".format(var),
        figure={"data": traces, "layout": {"title": {"text": var}}},
    )
    return plot


def create_prop_change_list(data, var):
    data_values = [(pd.Series(c) / pd.Series(c).sum()).to_dict() for c in data["value"]]
    traces = []
    unique_vals = reduce(lambda x, y: x.union(y), [set(i.keys()) for i in data_values])
    potential_lines = []
    for cat in unique_vals:
        values = pd.Series([i.get(cat, 0) for i in data_values])
        running_means = values.rolling(window=5).mean()
        differences = values - running_means
        sum_abs = np.abs(differences).sum()
        potential_lines.append((cat, differences, sum_abs))
    potential_lines = sorted(potential_lines, key=lambda v: v[2], reverse=True)

    for line in potential_lines[:5]:
        traces.append(dict(x=data["date"], y=line[1], mode="lines", name=line[0]))

    plot = dcc.Graph(
        id="prop-change-graph-{}".format(var),
        figure={
            "data": traces,
            "layout": {
                "title": {"text": "Difference in proportion over time (top 5) "}
            },
        },
    )
    return plot
