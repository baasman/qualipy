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
