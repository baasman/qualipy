import pandas as pd
import numpy as np
import altair as alt


def plot_correlation(corr_data, title, strongest_only=True):
    if strongest_only:
        strongest = corr_data.sort_values("Correlation", ascending=False)
        strongest = strongest[strongest["Variable 1"] != strongest["Variable 2"]]
        corr_data = corr_data[
            (corr_data["Variable 1"].isin(strongest["Variable 1"].head(20).unique()))
            & (corr_data["Variable 2"].isin(strongest["Variable 1"].head(20).unique()))
        ]

    base = (
        alt.Chart(corr_data)
        .encode(
            x="Variable 2:O", y="Variable 1:O", tooltip=["Variable 1", "Variable 2"]
        )
        .properties(height=500, width=500, title=title)
    )

    text = base.mark_text().encode(
        text="Correlation",
        color=alt.condition(
            alt.datum.Correlation > 0.5, alt.value("white"), alt.value("black")
        ),
    )

    cor_plot = base.mark_rect().encode(color="Correlation:Q")
    cor_plot = cor_plot + text
    return cor_plot


def numeric_batch_profile(data, title):
    plots = []
    for domain in data.run_name.unique():
        tmp = data[data.run_name == domain]
        chart = alt.Chart(tmp)
        chart = chart.mark_bar().encode(
            x=alt.X("column_name:N"),
            y=alt.Y("value:Q"),
            tooltip=["value", "column_name"],
        )

        chart = chart.properties(title=domain, width=20 * tmp.shape[0])
        plots.append(chart)
    final_chart = (
        alt.hconcat(*plots).resolve_scale(y="independent").properties(title=title)
    )
    return final_chart


def histogram_from_custom_bins(data):
    histd = pd.DataFrame({"counts": data[0]})
    histd["bin_min"] = data[1][:-1]
    histd["bin_max"] = data[1][1:]
    chart = (
        alt.Chart(histd)
        .mark_bar()
        .encode(
            x=alt.X("bin_min", bin="binned", axis=alt.Axis(title="Value")),
            x2="bin_max",
            y="counts",
            tooltip=["counts", "bin_min", "bin_max"],
        )
    )
    chart = chart.properties(height=200, width=400)
    return chart


def barchart_top_categories_from_value_counts(data, frequencies):
    data = pd.DataFrame({"Variable": list(data.keys()), "Count": list(data.values())})
    freqs = pd.DataFrame(
        {"Variable": list(frequencies.keys()), "Freq": list(frequencies.values())}
    )
    freqs.Freq = freqs.Freq.round(3)
    data = data.merge(freqs)
    chart = (
        alt.Chart(data)
        .mark_bar()
        .encode(
            y=alt.Y("Variable:N", sort="-x"),
            x=alt.X("Count:Q"),
            tooltip=["Variable", "Count"],
        )
        .properties(width=400)
    )
    text = chart.mark_text(align="left", baseline="middle", dx=3).encode(text="Freq:Q")
    chart = chart + text
    return chart


def barchart_from_dict(data):
    data = pd.DataFrame({"Variable": list(data.keys()), "Count": list(data.values())})
    chart = (
        alt.Chart(data)
        .mark_bar()
        .encode(
            y=alt.Y("Variable:N", sort="-x"),
            x=alt.X("Count:Q"),
            tooltip=["Variable", "Count"],
        )
        .properties(width=400)
    )
    return chart