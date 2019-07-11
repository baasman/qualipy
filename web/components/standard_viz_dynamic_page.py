from functools import reduce

import dash_core_components as dcc


def create_value_count_area_chart(data, var):
    data_values = data["value"].tolist()
    traces = []
    unique_vals = reduce(lambda x, y: x.union(y), [set(i.keys()) for i in data_values])
    x = data["date"]
    for value in unique_vals:
        traces.append(
            dict(
                x=data["batch_name"],
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
