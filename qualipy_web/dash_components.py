import dash_core_components as dcc
import dash_table


def column_choice(column_options, id="column-choice", multi=True):
    return dcc.Dropdown(
        id=id,
        options=[{"label": i, "value": i} for i in column_options],
        value=column_options[0],
        multi=multi,
        style={"width": "300px", "marginTop": "30px"},
    )


def view_style(id="view-choice", multi=False):
    return dcc.Dropdown(
        id=id,
        options=[{"label": i, "value": i} for i in ["simple", "detailed"]],
        value="simple",
        multi=multi,
        style={"width": "300px", "marginTop": "30px"},
    )


def batch_choice(batches, id, include_all=True, multi=True):
    if include_all:
        options = [{"label": i, "value": i} for i in batches] + [
            {"label": "all", "value": "all"}
        ]
        value = "all"
    else:
        options = [{"label": i, "value": i} for i in batches]
        value = options[-1]
    return dcc.Dropdown(
        id=id,
        options=options,
        value=value,
        multi=multi,
        style={"width": "300px", "marginTop": "30px"},
    )


def alerts_markdown(data):
    lines = []
    for idx, row in data.iterrows():
        lines.append(
            "Column {} has an anomaly in batch {}".format(row["column"], row["date"])
        )

    warning_lines = "\n\n".join(lines)
    print(warning_lines)

    markdown = dcc.Markdown(
        """
        Alerts
        =======
        
        
        {}
        
    """.format(
            warning_lines
        )
    )
    return markdown
