import pandas as pd
import numpy as np
import plotly.graph_objects as go
from qualipy.anomaly_detection import AnomalyModel
from functools import reduce
from collections import Counter
from plotly.subplots import make_subplots


def value_count_chart(data, var, anom_data):
    metric = data.metric.iloc[0]
    data_values = [(pd.Series(c) / pd.Series(c).sum()).to_dict() for c in data["value"]]
    traces = []
    unique_vals = reduce(lambda x, y: x.union(y), [set(i.keys()) for i in data_values])

    for value in unique_vals:
        traces.append(
            go.Scatter(
                x=data["date"],
                y=[i.get(value, 0) for i in data_values],
                hoverinfo="x+y",
                mode="lines",
                line=dict(width=0.5),
                stackgroup="one",
                name=value,
            )
        )

    shapes = []
    for idx, row in anom_data.iterrows():
        shapes.append(
            {
                "type": "line",
                "x0": row["date"],
                "x1": row["date"],
                "y0": 0,
                "y1": 1,
                "line": {"color": "red", "width": 4},
            }
        )
    plot = go.Figure(
        data=traces, layout={"title_text": f"{var}_{metric}", "shapes": shapes}
    )
    plot.show()


def barchart_top_categories(data):
    counter = Counter(data.value.values[0])
    for vc in data.value.values[1:]:
        counter += Counter(vc)
    items = counter.most_common()
    x = [i[0] for i in items]
    y = [i[1] for i in items]
    plot = go.Figure(
        data=[go.Bar(x=x, y=y)],
        layout=go.Layout(title="All Categories", xaxis={"type": "category"}),
    )
    plot.show()


def proportion_change(data, var):
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

    try:
        all_lines = pd.DataFrame({i[0]: i[1] for i in potential_lines})
        mod = AnomalyModel()
        outliers = mod.train_predict(all_lines.values[4:])
        outliers = np.concatenate([np.array([1, 1, 1, 1]), outliers])
        outliers = [0 if i == -1 else np.NaN for i in outliers]
    except:
        outliers = np.repeat(np.NaN, len(data_values))

    for line in potential_lines[:5]:
        traces.append(dict(x=data["date"], y=line[1], mode="lines", name=line[0]))
    traces.append(
        dict(
            x=data["date"],
            y=outliers,
            name="Outlier",
            mode="markers",
            marker=dict(color="rgb(164, 4, 4)", size=10),
        )
    )

    plot = go.Figure(
        data=traces,
        layout={"title": {"text": "Difference in proportion over time (top 5) "}},
    )
    plot.show()