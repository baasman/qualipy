from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.serving import run_simple
import flask
from flask.cli import load_dotenv
from flask import request, url_for, render_template, redirect, session
from dash import Dash
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
from sqlalchemy import create_engine

import os
import json

from qualipy_web.config import Config
from qualipy_web.components.data_characteristic_page import (
    create_type_plots,
    create_simple_line_plot_subplots,
    create_unique_columns_plot,
    bar_plot_missing,
)
from qualipy_web.layout import generate_layout
from qualipy_web.components.overview_page import overview_table, schema_table
from qualipy_web.components.numerical_page import (
    histogram,
    create_trend_line,
    all_trends,
)
from qualipy_web.components.boolean_page import error_check_table, boolean_plot
from qualipy_web.components.standard_viz_dynamic_page import (
    create_value_count_area_chart,
)
from qualipy_web.components.standard_viz_static_page import heatmap
from qualipy.database import get_table, get_project_table, get_last_time
from qualipy.util import set_value_type

### Middleware


class PrefixMiddleware(object):
    def __init__(self, app_mw, prefix=""):
        self.app = app_mw
        self.prefix = prefix

    def __call__(self, environ, start_response):

        if environ["PATH_INFO"].startswith(self.prefix):
            environ["PATH_INFO"] = environ["PATH_INFO"][len(self.prefix) :]
            environ["SCRIPT_NAME"] = self.prefix
            return self.app(environ, start_response)
        else:
            start_response("404", [("Content-Type", "text/plain")])
            return ["This url does not belong to the app.".encode()]


###

server = flask.Flask(__name__)
# server.wsgi_app = PrefixMiddleware(server.wsgi_app, prefix="/qualipy")
load_dotenv()

server.config.from_object(Config)
server.config.update(DEBUG=True, SECRET_KEY="supersecrit")
config = Config()


HOME = os.path.expanduser("~")


dash_app1 = Dash(__name__, server=server, url_base_pathname="/metrics/")
dash_app1.config["suppress_callback_exceptions"] = True
dash_app1.layout = html.Div([])

full_data = pd.DataFrame()
last_date = None


def select_data(
    project, column=None, batch=None, url=None, live_update=False, n_intervals=0
):
    global full_data
    global last_date
    data = full_data.copy()
    engine = create_engine(url)

    print("selecting data")
    print(data.shape)
    try:
        if data.shape[0] == 0:
            data = get_project_table(engine, project)
        else:
            data = full_data.copy()
    except:
        raise Exception("Can't find any data at {}".format(url))

    if n_intervals == 0:
        last_date = pd.to_datetime(data.insert_time.iloc[-1]) + pd.Timedelta(seconds=3)
    if live_update and n_intervals > 0:
        new_last_time = pd.to_datetime(get_last_time(engine, project))
        if new_last_time > last_date:
            new_data = get_project_table(engine, project, last_date)
            data = pd.concat([data, new_data])

            full_data = data
            last_date = new_last_time

    if column is not None:
        if not isinstance(column, str):
            data = data[data["column_name"].isin(column)]
        else:
            data = data[data["column_name"] == column]
    data = data.sort_values(["column_name", "metric", "date"])

    if batch is None or batch == "all":
        print(data.shape)
        return data
    else:
        if not isinstance(batch, list):
            batch = [batch]
        try:
            batch_values = [i["value"] for i in batch]
        except TypeError:
            batch_values = batch
        data = data[data["date"].isin(batch_values)]
        print(data.shape)
        return data


#### Overview Tab ####


@dash_app1.callback(
    Output(component_id="overview-page-results", component_property="children"),
    [
        Input(component_id="placeholder", component_property="n_clicks"),
        Input(component_id="live-refresh", component_property="n_intervals"),
    ],
)
def update_tab_1(n_clicks, n_intervals):
    data = select_data(
        session["project"],
        batch="all",
        column=None,
        url=session["db_url"],
        live_update=True,
        n_intervals=n_intervals,
    )
    data = data[data.type == "data-characteristic"]
    row = [
        session["project"],
        data[data["column_name"] == "rows"].value.astype(int).sum(),
        data[data["column_name"] == "columns"].value.astype(int).iloc[-1],
        data["date"].min(),
        data["date"].nunique(),
    ]
    over_table = pd.DataFrame(
        [row],
        columns=[
            "project",
            "number_of_rows",
            "number_of_columns",
            "last_run_time",
            "number_of_batches",
        ],
    )
    schema = pd.DataFrame(
        [
            {
                "column": col,
                "type": info["dtype"],
                "null": info["nullable"],
                "unique": info["unique"],
            }
            for col, info in session["schema"].items()
        ]
    )
    schema["null"] = schema["null"].astype(str)
    schema["unique"] = schema["unique"].astype(str)

    title_data = html.H4("Data characteristics")
    data_char = overview_table(over_table)
    schema_title = html.H4("Schema")
    schema = schema_table(schema)

    overview_div = html.Div(
        id="overview-home", children=[title_data, data_char, schema_title, schema]
    )
    page = html.Div(id="home-page", children=[overview_div])
    return page


#### Numerical Trends ####


@dash_app1.callback(
    Output(component_id="numerical-page-results", component_property="children"),
    [
        Input(component_id="numerical-page-col-choice", component_property="value"),
        Input(component_id="numerical-page-view-choice", component_property="value"),
        Input(component_id="live-refresh-num", component_property="n_intervals"),
    ],
)
def update_tab_2(column, view, n_intervals):
    plots = []

    if view == "detailed":
        data = select_data(
            session["project"],
            column=column,
            url=session["db_url"],
            live_update=True,
            n_intervals=n_intervals,
        )
        data = data[data["type"] == "numerical"]
        for idx, metric in enumerate(
            data[data["column_name"] == column]["metric"].unique()
        ):
            args = data[data["metric"] == metric]["arguments"].iloc[0]
            metric_title = metric if args is None else "{}_{}".format(metric, args)
            plot_data = data[
                (data["column_name"] == column) & (data["metric"] == metric)
            ]
            plot_data = set_value_type(plot_data)
            plots.append(
                html.Div(
                    id="trend-plots-{}".format(idx),
                    children=[
                        html.H3("{}-{}".format(column, metric_title, id="plot-header")),
                        create_trend_line(
                            plot_data, column, metric, project_name=session["project"]
                        ),
                        histogram(plot_data, column, metric),
                    ],
                )
            )
    elif view == "simple":
        data = select_data(
            session["project"],
            column=column,
            url=session["db_url"],
            live_update=True,
            n_intervals=n_intervals,
        )
        data = data[data["type"] == "numerical"]
        all_trends_view = all_trends(data)
        plots.append(html.Div(children=[all_trends_view]))
    elif view == "key metrics":
        data = select_data(
            session["project"],
            column=None,
            url=session["db_url"],
            live_update=True,
            n_intervals=n_intervals,
        )
        data = data[(data["type"] == "numerical") & (data["key_function"] == 1)]
        all_key_trends = all_trends(data, show_column_in_name=True)
        plots.append(html.Div(children=[all_key_trends]))
    return plots


#### standard_viz_dynamic ####


@dash_app1.callback(
    Output(component_id="categorical-page-results", component_property="children"),
    [
        Input(
            component_id="categorical-page-col-choice-multi", component_property="value"
        )
    ],
)
def update_tab_3(column):
    data = select_data(session["project"], column=column, url=session["db_url"])

    data = data[(data["type"] == "standard_viz_dynamic")]

    plots = {"value_counts": create_value_count_area_chart}
    dynamic_plots = []
    # TODO: have to completely rework this
    for metric in data.metric.unique():
        plot_data = data[data.metric == metric]
        plot = plots[metric](plot_data, column)
        dynamic_plots.append(plot)
    dynamic_plot_div = html.Div(id="d-plots", children=dynamic_plots)

    plot_div = html.Div(
        id="value-count-section", children=[html.H3("Plots"), dynamic_plot_div]
    )
    return plot_div


#### Data Characteristics tab ####


@dash_app1.callback(
    Output(component_id="tab-4-results", component_property="children"),
    [
        Input(component_id="batch-choice-4", component_property="value"),
        Input(component_id="live-refresh-cat", component_property="n_intervals"),
    ],
)
def update_tab_4(batch, n_intervals):
    data = select_data(
        session["project"],
        column=None,
        batch=batch,
        url=session["db_url"],
        live_update=True,
        n_intervals=n_intervals,
    )
    type_plot = create_type_plots(data[(data["metric"] == "dtype")].copy())
    missing_plot = bar_plot_missing(
        data[(data["metric"] == "perc_missing")].copy(), session["schema"]
    )
    rows_columns = create_simple_line_plot_subplots(data.copy())
    unique_plot = create_unique_columns_plot(
        data[(data["metric"] == "is_unique")].copy(), session["schema"]
    )

    # plots
    line_plots = html.Div(id="data-line-plots", children=[rows_columns])

    missing = html.Div(id="missing", children=[missing_plot])

    data_type_plots = html.Div(id="data-type-plots", children=[type_plot])

    unique_plot = html.Div(id="unique-plot", children=[unique_plot])

    ###### row 1

    row1 = html.Div(id="row1-data-char", children=[line_plots, unique_plot])
    ###### row 2
    row2 = html.Div(id="row2-data-char", children=[missing])

    page = html.Div(id="overview-page", children=[row1, row2])
    return page


#### Single Batch analyzer tab ####


@dash_app1.callback(
    Output(component_id="tab-5-results", component_property="children"),
    [
        Input(component_id="batch-choice-5", component_property="value"),
        Input(component_id="tab-5-col-choice", component_property="value"),
    ],
)
def update_tab_5(batch, column):
    data = select_data(
        session["project"], column=column, batch=batch, url=session["db_url"]
    )

    data = data[(data["type"] == "standard_viz_static")]

    plots = {"heatmap": heatmap, "correlation": heatmap}
    static_plots = []
    for metric in data["metric"].unique():
        plot_data = data[(data["metric"] == metric)]
        plot = plots[metric](plot_data, column)
        static_plots.append(plot)

    page = html.Div(id="tab-5-page", children=static_plots)
    return page


#### Boolean graphs tab ####


@dash_app1.callback(
    Output(component_id="bool-tab-results", component_property="children"),
    [Input(component_id="bool-tab-col-choice", component_property="value")],
)
def update_tab_6(column):
    data = select_data(
        session["project"], column=column, batch=None, url=session["db_url"]
    )
    error_data = select_data(
        session["project"], column=None, batch=None, url=session["db_url"]
    )
    error_data = error_data[error_data.type == "boolean"]
    error_data = set_value_type(error_data)
    error_data = error_data[error_data.value == False]

    error_data = error_data.sort_values("date", ascending=False)

    data = data[(data["type"] == "boolean")]
    plot = boolean_plot(data)
    error_table = error_check_table(
        error_data[["column_name", "metric", "arguments", "batch_name"]]
    )

    table_header = html.H3("All Failed Checks")

    row1 = html.Div(id="row1", children=[plot, table_header, error_table])

    page = html.Div(id="tab-6-page", children=row1)
    return page


@server.route("/", methods=["GET", "POST"])
@server.route("/index", methods=["GET", "POST"])
def index():
    global dash_app1
    global full_data

    session.clear()

    project_file = os.getenv(
        "PROJECT_FILE", os.path.join(HOME, ".qualipy", "projects.json")
    )

    config_file = os.getenv(
        "CONFIG_FILE", os.path.join(HOME, ".qualipy", "config.json")
    )

    with open(project_file, "r") as f:
        projects = json.loads(f.read())

    with open(config_file, "r") as f:
        config = json.loads(f.read())

    if request.method == "POST":
        button_pressed = list(request.form.to_dict(flat=False).keys())[0]
        session["project"] = button_pressed
        column_options = projects[button_pressed]["columns"]
        session["column_options"] = column_options
        url = projects[button_pressed].get("db", None)
        if "db_url" in config and not url:
            url = config["db_url"]
        if url is None:
            raise ValueError

        session["db_url"] = url
        session["schema"] = projects[button_pressed]["schema"]

        full_data = select_data(session["project"], url=url)

        numerical_options = full_data[
            full_data["type"] == "numerical"
        ].column_name.unique()
        standard_viz_dynamic_options = full_data[
            full_data["type"] == "standard_viz_dynamic"
        ].column_name.unique()
        boolean_options = full_data[full_data["type"] == "boolean"].column_name.unique()
        interval_time = config.get("interval_time", 100000)

        dash_app1.layout = html.Div(
            id="total-div",
            children=generate_layout(
                data=full_data,
                numerical_column_options=numerical_options,
                standard_viz_dynamic_options=standard_viz_dynamic_options,
                boolean_options=boolean_options,
                interval_time=interval_time,
            ),
        )
        return redirect(url_for("render_dashboard"))
    return render_template("home/index.html", projects=projects)


@server.route("/metrics")
def render_dashboard():
    return redirect("/dash1")


app = DispatcherMiddleware(server, {"/dash1": dash_app1.server})

dash_app1.css.append_css(
    {
        "external_url": (
            "https://github.com/plotly/dash-app-stylesheets/blob/master/dash-analytics-report.css"
        )
    }
)

if __name__ == "__main__":
    run_simple("localhost", 5006, app, use_reloader=True, use_debugger=True)
