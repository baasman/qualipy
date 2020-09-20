import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import altair as alt

from qualipy.util import set_value_type

from collections import Counter
from functools import reduce
from itertools import combinations


def comparison_trends_altair(data, columns, metrics, show_column_in_name=False):
    df = data.copy()
    df = df[(df.column_name.isin(columns)) & (df.metric.isin(metrics))]
    df["metric_name"] = df.metric.astype(str) + np.where(
        df.arguments.isnull(), "", df.arguments
    )
    if show_column_in_name:
        df.metric_name = df.metric_name + "-" + df.column_name
    col_name = df.column_name.values[0]
    comparison_column_name = " - ".join(columns)
    df = set_value_type(df)
    df = df.sort_values(["metric_name", "date"])

    df["mean_value"] = df.groupby(["column_name", "metric_name"]).value.transform(
        "mean"
    )
    df["std_value"] = df.groupby(["column_name", "metric_name"]).value.transform("std")
    df["standardized_value"] = (df.value - df.mean_value) / df.std_value
    for row, (name, group) in enumerate(df.groupby("metric_name"), start=1):

        chart = alt.Chart(group).properties(
            width=800,
            height=200,
            title=f"Comparison of values - {comparison_column_name} - {name}",
        )
        chart = chart.mark_line().encode(
            x="date:T",
            y="value:Q",
            color="column_name",
            tooltip=["value", "column_name"],
        )

        std_chart = alt.Chart(group).properties(
            width=800, height=200, title="Standardized values"
        )
        std_chart = std_chart.mark_line().encode(
            x="date:T",
            y="standardized_value:Q",
            color="column_name",
            tooltip=["value", "column_name"],
        )

        plot = (
            alt.vconcat(chart, std_chart)
            .resolve_axis(y="shared")
            .configure_legend(orient="bottom")
        )
        plot.display()


def plot_diffs_altair(data, metrics, time_freq="1D", show_notebook=True):
    sub_data = []
    for sub, metric in zip(data, metrics):
        subd = sub[sub.metric_id == metric].copy()
        if subd.shape[0] == 0:
            raise Exception(f"Unable to find {metric} in data. Check the order")
        subd = set_value_type(subd)
        subd["standardized"] = (subd.value - subd.value.mean()) / subd.value.std()
        subd["value_diff"] = subd.value.diff()
        sub_data.append(subd)
    pairs = list(combinations(metrics, 2))

    df = pd.concat(sub_data)

    # if need to resample
    diff_df = df.set_index("date")
    resampled = (
        diff_df.groupby("metric_id")
        .value.resample(rule=time_freq)
        .last()
        .reset_index()
        .pivot(columns="metric_id", index="date")
    )
    resampled.columns = [i[1] for i in resampled.columns]
    resampled = resampled.reset_index()

    title = " - ".join(metrics)
    chart = alt.Chart(df).properties(width=800, height=200, title=title)
    chart = chart.mark_line().encode(
        x="date:T", y="value:Q", color="metric_id", tooltip=["value", "metric_id"]
    )

    std_chart = alt.Chart(df).properties(width=800, height=100)
    std_chart = std_chart.mark_line().encode(
        x="date:T",
        y="standardized:Q",
        color="metric_id",
        tooltip=["value", "metric_id"],
    )

    value_diff_chart = alt.Chart(df).properties(width=800, height=100)
    value_diff_chart = value_diff_chart.mark_line().encode(
        x="date:T", y="value_diff:Q", color="metric_id", tooltip=["value", "metric_id"]
    )

    plot = alt.vconcat(chart, std_chart, value_diff_chart).resolve_axis(y="shared")

    pair_plots = []
    for pair in pairs:
        pair_name = " - ".join(pair)
        resampled[pair_name] = resampled[pair[0]] - resampled[pair[1]]
        pair_plot = (
            alt.Chart(resampled[["date", pair_name]])
            .mark_line()
            .encode(
                x="date:T",
                y=alt.Y(
                    f"{pair_name}:Q", axis=alt.Axis(labels=True, title="Difference")
                ),
                tooltip=f"{pair_name}:Q",
            )
            .properties(width=800, height=100, title=pair_name)
        )
        pair_plots.append(pair_plot)
    pair_plots = alt.vconcat(*pair_plots).resolve_axis(x="shared")

    total = alt.vconcat(plot, pair_plots)
    return total


def get_counts(data, metric_id, top_n=20):
    counter = Counter(data.value.values[0])
    for vc in data.value.values[1:]:
        counter += Counter(vc)
    items = counter.most_common()
    x = [i[0] for i in items][:top_n]
    y = [i[1] for i in items][:top_n]
    counts = pd.DataFrame({"category": x, "value": y})
    counts["metric"] = metric_id
    return counts


def bar_chart_comparison_altair(
    data: list, metrics: list, top_n=10, only_overlapping=False, show_notebook=True
):
    counts = []
    for df, metric in zip(data, metrics):
        df_ = df[df.metric_id == metric].copy()
        df_ = set_value_type(df_)
        counts.append(get_counts(df_, metric))

    df = pd.concat(counts)

    title = " - ".join(metrics)
    chart = alt.Chart(df).properties(height=50, title=title)
    chart = chart.mark_bar().encode(
        y=alt.Y("metric:N", axis=alt.Axis(title=None, labels=False)),
        x=alt.X("value:Q"),
        row=alt.Row("category:N", header=alt.Header(labelAngle=0, labelAlign="left")),
        color="metric:N",
        tooltip=["value", "metric"],
    )
    plot = chart.configure_view(strokeOpacity=0)
    if show_notebook:
        plot.display()
    else:
        return plot


def get_cat_lines(data, top_n=10):
    data_values = [pd.Series(c) for c in data["value"]]
    unique_vals = reduce(lambda x, y: x.union(y), [set(i.keys()) for i in data_values])
    df = pd.DataFrame(
        {cat: [i.get(cat, 0) for i in data_values] for cat in unique_vals}
    )
    if df.shape[0] == 0:
        return
    top_columns = (
        df.sum().sort_values(ascending=False).head(min(len(unique_vals), top_n))
    )
    df = df[top_columns.index]
    return df


def create_column_data(data, column):
    if column in data.columns:
        cdata = data[[column] + ["date", "metric"]]
    else:
        cdata = pd.DataFrame(columns=[column, "date", "metric"])
    return cdata


def value_count_comparison_altair(
    datasets: list, metrics: list, top_n=10, only_overlapping=False, show_notebook=True
):
    dfs = []
    for data, metric in zip(datasets, metrics):
        d = data[data.metric_id == metric].copy()
        d = set_value_type(d)
        df = get_cat_lines(d)
        df["metric"] = metric
        df["date"] = d["date"].values
        dfs.append(df)

    all_columns = reduce(lambda x, y: x.union(y), [set(i.columns) for i in dfs])

    lines = []
    for column in [i for i in all_columns if i not in ["metric", "date"]]:
        sub_dfs = []
        for df in dfs:
            sub_dfs.append(create_column_data(df, column))
        subdata = pd.concat(sub_dfs)
        line = (
            alt.Chart(subdata)
            .mark_line()
            .encode(
                x="date:T",
                y=f"{column}:Q",
                color="metric:N",
                tooltip=[column, "metric"],
            )
            .properties(height=100, width=800)
        )
        lines.append(line)

    plot = alt.vconcat(*lines).resolve_axis(y="shared")
    if show_notebook:
        plot.display()
    else:
        return plot
