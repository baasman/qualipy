import pandas as pd
import numpy as np
from functools import reduce
from collections import Counter
import altair as alt


def barchart_top_categories_altair(data, column, top_n=20, show_notebook=True):
    counter = Counter(data.value.values[0])
    for vc in data.value.values[1:]:
        counter += Counter(vc)
    items = counter.most_common()
    x = [i[0] for i in items][:top_n]
    y = [i[1] for i in items][:top_n]
    d = pd.DataFrame({"category": x, column: y})
    if d.shape[0] == 0:
        return
    chart = (
        alt.Chart(d)
        .mark_bar()
        .encode(
            x=alt.X("category:N", sort="-y"),
            y=alt.Y(f"{column}:Q"),
            tooltip=["category", column],
        )
        .properties(title="All Categories", width=600)
    )
    if show_notebook:
        chart.display()
    else:
        return chart


def value_count_chart_altair(data, var, anom_data, top_n=20, show_notebook=True):
    prop_change_plot = proportion_change_altair(data, var, top_n)
    metric = data.metric.iloc[0]
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
    df["date"] = data["date"].values
    df = df.melt("date")
    df["batch_name"] = data["batch_name"].tolist() * top_columns.shape[0]
    df["anom"] = np.where(df.batch_name.isin(anom_data.batch_name), 1, 0)
    base = alt.Chart(df).properties(
        title=f"{var} - {metric}: Proportion over time", width=600
    )
    area = base.mark_area().encode(
        x=alt.X("date:T"),
        y=alt.Y("value:Q", stack="normalize"),
        color="variable:N",
        tooltip=["value", "date", "variable"],
    )
    anom_line = (
        alt.Chart(df).mark_rule(color="red").encode(x="date:T", y=alt.Y("anom:Q"))
    )
    plot = alt.layer(area, anom_line)
    final_plot = alt.vconcat(plot, prop_change_plot).resolve_axis(y="shared")
    if show_notebook:
        final_plot.display()
    else:
        return final_plot


def proportion_change_altair(data, var, top_n=20):
    data_values = [
        (pd.Series(c, dtype="int") / pd.Series(c, dtype="int").sum()).to_dict()
        for c in data["value"]
    ]
    traces = []
    unique_vals = reduce(lambda x, y: x.union(y), [set(i.keys()) for i in data_values])
    diffs = []
    cats = []
    dates = []
    for cat in unique_vals:
        values = pd.Series([i.get(cat, 0) for i in data_values])
        running_means = values.rolling(window=5).mean()
        differences = (values - running_means).tolist()
        categories = np.repeat(cat, len(differences)).tolist()
        diffs.extend(differences)
        cats.extend(categories)
        dates.extend(data["date"].tolist())

    df = pd.DataFrame({"date": dates, "category": cats, "proportional change": diffs})
    top_cats = (
        df.groupby("category")["proportional change"]
        .sum()
        .head(min(len(unique_vals), top_n))
    )
    df = df[df.category.isin(top_cats.index)]

    base = alt.Chart(df).properties(title="Change in Proportion", width=600)
    lines = base.mark_line().encode(
        x=alt.X("date:T"), y=alt.Y("proportional change:Q"), color="category:N"
    )
    return lines
