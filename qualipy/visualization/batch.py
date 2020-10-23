import pandas as pd
import numpy as np
import altair as alt


def plot_correlation(corr_data, title, strongest_only=True):
    if strongest_only:
        strongest = corr_data.sort_values("Correlation", ascending=False)
        strongest = strongest[strongest["Variable 1"] != strongest["Variable 2"]]
        corr_data = corr_data[
            (corr_data["Variable 1"].isin(strongest["Variable 1"].unique()[:20]))
            & (corr_data["Variable 2"].isin(strongest["Variable 1"].unique()[:20]))
        ]
        corr_data.Correlation = corr_data.Correlation.round(2)

    base = (
        alt.Chart(corr_data)
        .encode(
            x="Variable 2:O", y="Variable 1:O", tooltip=["Variable 1", "Variable 2"]
        )
        .properties(height=600, width=600, title=title)
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


def histogram_from_custom_bins(data, num_facets):
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
    chart = chart.properties(height=200, width=400, title="Full Batch")
    if num_facets:
        hists = []
        for key in list(num_facets.keys()):
            sub_hist = pd.DataFrame({"counts": num_facets[key][0]})
            sub_hist["bin_min"] = num_facets[key][1][:-1]
            sub_hist["bin_max"] = num_facets[key][1][1:]
            sub_hist["category"] = key
            hists.append(sub_hist)
        facet_hist = pd.concat(hists)

        facet_chart = (
            alt.Chart(facet_hist)
            .mark_bar(opacity=0.5)
            .encode(
                x=alt.X("bin_min", bin="binned", axis=alt.Axis(title="Value")),
                x2="bin_max",
                y="counts",
                color="category:N",
                tooltip=["counts", "bin_min", "bin_max"],
            )
        )
        facet_chart = facet_chart.properties(height=200, width=400, title=f"Faceted")
        chart = alt.vconcat(chart, facet_chart)
    return chart


def barchart_top_categories_from_value_counts(data, frequencies, cat_facets=None):
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
    if cat_facets:
        facets = (
            pd.DataFrame(cat_facets)
            .T.reset_index()
            .rename(columns={"index": "Variable"})
        )
        facets = facets.melt(
            id_vars="Variable",
            value_vars=[col for col in facets.columns if col != "Variable"],
        ).rename(columns={"variable": "facet"})
        faceted_chart = (
            alt.Chart(facets)
            .mark_bar()
            .encode(
                x="sum(value)",
                y=alt.Y("Variable", sort="x"),
                tooltip=["facet", "Variable", "sum(value)"],
                color="facet",
            )
        )
        faceted_chart = faceted_chart.properties(title="Faceted by variable", width=400)
        chart = alt.vconcat(chart, faceted_chart)
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


def barchart_from_dict_on_dates(data):
    data = pd.DataFrame({"Variable": list(data.keys()), "Count": list(data.values())})
    mid_index = data[data.Count == data.Count.max()].index.values[0]
    min_index = max(0, mid_index - 20)
    max_index = min(mid_index + 20, data.shape[0])
    data = data.iloc[min_index:max_index]
    chart = (
        alt.Chart(data)
        .mark_bar()
        .encode(
            y=alt.Y("Count:Q"),
            x=alt.X("Variable:N"),
            tooltip=["Variable", "Count"],
        )
    ).properties(height=200, width=600)
    return chart
