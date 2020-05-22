import traceback
import os
import json
from functools import reduce
from collections import Counter

import qualipy
from qualipy.anomaly_detection import (
    LoadedModel,
    anomaly_data_all_projects,
    AnomalyModel,
)
from qualipy.util import set_value_type
from plotly.subplots import make_subplots

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import altair as alt


def trend_line(data, var, metric, config_dir, project_name, anom_data):
    title = "{}_{}".format(var, data.arguments.iloc[0])
    main_line = data.value
    mean = main_line.mean()
    median = main_line.median()
    std = main_line.std()
    mean_line = np.repeat(mean, main_line.shape[0])
    median_line = np.repeat(median, main_line.shape[0])
    std_line_lower = np.repeat(mean - (2 * std), main_line.shape[0])
    std_line_higher = np.repeat(mean + (2 * std), main_line.shape[0])

    x_axis = data["date"]

    if anom_data.shape[0] > 0:
        data = data.merge(
            anom_data[["column_name", "batch_name", "value"]].rename(
                columns={"value": "anom_val"}
            ),
            how="left",
            on=["column_name", "batch_name"],
        )
    else:
        data["anom_val"] = np.NaN

    plot_data = [
        go.Scatter(
            x=x_axis,
            y=main_line,
            name=title[:30],
            mode="lines+markers",
            marker=dict(line=dict(color="blue", width=2)),
        ),
        go.Scatter(
            x=x_axis,
            y=mean_line,
            name="mean",
            mode="lines",
            marker=dict(line=dict(width=1)),
            opacity=0.5,
        ),
        go.Scatter(
            x=x_axis,
            y=data.anom_val,
            name="Outliers",
            mode="markers",
            marker=dict(color="red", size=10),
        ),
        go.Scatter(
            x=x_axis,
            y=median_line,
            name="median",
            mode="lines",
            marker=dict(line=dict(width=1)),
            opacity=0.5,
        ),
        go.Scatter(
            x=x_axis,
            y=std_line_lower,
            name="-2 std",
            mode="lines",
            marker=dict(line=dict(width=1)),
            opacity=0.5,
        ),
        go.Scatter(
            x=x_axis,
            y=std_line_higher,
            name="+2 std",
            mode="lines",
            marker=dict(line=dict(width=1)),
            opacity=0.5,
        ),
    ]
    layout = {
        "title_text": f"{title} - {metric}",
        "yaxis": {"title": "value"},
        "xaxis": dict(
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1m", step="month", stepmode="backward",),
                        dict(count=6, label="6m", step="month", stepmode="backward",),
                        dict(step="all"),
                    ]
                )
            ),
            rangeslider=dict(visible=True),
            type="date",
        ),
    }

    plt = go.Figure(data=plot_data, layout=layout)
    plt.show()


def trend_line_altair(
    trend_data, var_name, metric_name, config_dir, project_name, anom_data, point=True
):
    if anom_data.shape[0] > 0:
        trend_data = trend_data.merge(
            anom_data[["column_name", "batch_name", "value"]].rename(
                columns={"value": "anom_val"}
            ),
            how="left",
            on=["column_name", "batch_name"],
        )
        trend_data["anom_val"] = trend_data["anom_val"].astype(float)
    else:
        trend_data["anom_val"] = np.NaN

    trend_data["2std_plus_line"] = trend_data.value.mean() + (
        2 * trend_data.value.std()
    )
    trend_data["2std_minus_line"] = trend_data.value.mean() - (
        2 * trend_data.value.std()
    )
    trend_data["mean_line"] = trend_data.value.mean()
    trend_data["median_line"] = trend_data.value.median()
    td = trend_data[
        [
            "date",
            "value",
            "2std_plus_line",
            "2std_minus_line",
            "mean_line",
            "median_line",
        ]
    ].melt("date")

    min_y = min(
        trend_data.value.min() - 0.001, trend_data["2std_minus_line"].iloc[0] - 0.001
    )
    max_y = max(
        trend_data.value.max() + 0.001, trend_data["2std_plus_line"].iloc[0] + 0.001
    )
    base = alt.Chart(td).properties(title=f"{var_name} - {metric_name}", width=800)
    value_line = base.mark_line(point=point).encode(
        x=alt.X("date:T"),
        y=alt.Y("value:Q", scale=alt.Scale(domain=[min_y, max_y])),
        color="variable:N",
        opacity=alt.condition(
            alt.datum.variable == "value", alt.value(1), alt.value(0.3)
        ),
    )
    anom_points = (
        alt.Chart(trend_data)
        .mark_point(size=50)
        .encode(x=alt.X("date:T"), y=alt.Y("anom_val"), color=alt.value("red"))
    )
    chart = value_line + anom_points
    chart.display()
