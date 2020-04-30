import pandas as pd
import numpy as np
import plotly.graph_objects as go
from qualipy.util import set_value_type
from plotly.subplots import make_subplots


def comparison_trends(data, columns, show_column_in_name=False):
    df = data.copy()
    df = df[df.column_name.isin(columns)]
    df["metric_name"] = df.metric.astype(str) + np.where(
        df.arguments.isnull(), "", df.arguments
    )
    if show_column_in_name:
        df.metric_name = df.metric_name + "-" + df.column_name
    col_name = df.column_name.values[0]
    df = df.sort_values(["metric_name", "date"])
    fig = make_subplots(
        rows=df.metric_name.nunique(),
        cols=1,
        shared_xaxes=True,
        shared_yaxes=False,
        vertical_spacing=0.1,
        subplot_titles=df.metric_name.unique().tolist(),
    )
    for row, (name, group) in enumerate(df.groupby("metric_name"), start=1):
        x = group.date
        group = set_value_type(group.copy())
        for col in group.column_name.unique():
            fig.append_trace(
                go.Scatter(
                    x=x, y=group[group.column_name == col].value.values, name=col
                ),
                row=row,
                col=1,
            )

    fig["layout"].update(
        title_text="Comparison between {}".format(
            "-".join(df.column_name.unique().tolist())
        ),
        showlegend=False,
    )
    fig.show()
