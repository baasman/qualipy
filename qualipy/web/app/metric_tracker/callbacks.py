import os

from dash.dependencies import Input, Output
import dash_html_components as html
import pandas as pd
from sqlalchemy import create_engine
from flask import request, url_for, render_template, redirect, session
from flask import current_app as capp

from qualipy.web.app.caching import (
    cache,
    cache_dataframe,
    get_cached_dataframe,
    set_session_data_name,
    set_session_anom_data_name,
)
from qualipy._sql import SQLite
from qualipy.util import set_value_type
from qualipy.web.app.metric_tracker.components.overview_page import (
    overview_table,
    schema_table,
    anomaly_num_data,
    anomaly_num_table,
)
from qualipy.web.app.metric_tracker.components.numerical_page import (
    histogram,
    create_trend_line,
    all_trends,
)
from qualipy.web.app.metric_tracker.components.standard_viz_dynamic_page import (
    create_value_count_area_chart,
    create_prop_change_list,
    barchart_top_cats,
)
from qualipy.web.app.metric_tracker.components.boolean_page import (
    error_check_table,
    boolean_plot,
)
from qualipy.web.app.metric_tracker.components.data_characteristic_page import (
    create_type_plots,
    create_simple_line_plot_subplots,
    create_unique_columns_plot,
    bar_plot_missing,
)


HOME = os.path.expanduser("~")


def select_data(
    project,
    column=None,
    batch=None,
    url=None,
    live_update=False,
    n_intervals=0,
    session_id=None,
):
    session_data_name = set_session_data_name(session_id)
    last_date = session.get("last_date", None)
    cached_data = get_cached_dataframe(session_data_name)
    if cached_data is None:
        cached_data = pd.DataFrame()
    data = cached_data.copy()
    engine = create_engine(url)
    try:
        if data.shape[0] == 0:
            data = SQLite.get_project_table(engine, project)
            cache_dataframe(data, session_data_name)
        else:
            data = cached_data.copy()
    except:
        raise Exception("Can't find any data at {}".format(url))
    if n_intervals == 0:
        last_date = pd.to_datetime(data.insert_time.iloc[-1]) + pd.Timedelta(seconds=3)
        session["last_date"] = last_date
    if live_update and n_intervals > 0:
        new_last_time = pd.to_datetime(SQLite.get_last_time(engine, project))
        if new_last_time > last_date:
            new_data = SQLite.get_project_table(engine, project, last_date)
            data = pd.concat([data, new_data])
            cache_dataframe(data, session_data_name)
            cache.set("last_date", new_last_time)

    if column is not None and column != "None":
        if not isinstance(column, str):
            data = data[data["column_name"].isin(column)]
        else:
            data = data[data["column_name"] == column]
    data = data.sort_values(["column_name", "metric", "date"])

    if batch is None or batch == "all":
        return data
    else:
        if not isinstance(batch, list):
            batch = [batch]
        try:
            batch_values = [i["value"] for i in batch]
        except TypeError:
            batch_values = batch
        data = data[data["date"].isin(batch_values)]
        return data


def register_callbacks(dashapp):
    @dashapp.callback(
        Output(component_id="numerical-page-col-choice", component_property="options"),
        [Input("session-id", "children")],
    )
    def numerical_columns(session_id):
        data = select_data(
            session["project_name"],
            batch="all",
            column=None,
            url=capp.config["QUALIPY_DB"],
            live_update=False,
            n_intervals=0,
            session_id=session_id,
        )
        columns = data[data.type == "numerical"].column_name.unique()
        options = [{"label": i, "value": i} for i in columns]
        return options

    @dashapp.callback(
        Output(
            component_id="categorical-page-col-choice-multi",
            component_property="options",
        ),
        [Input("session-id", "children")],
    )
    def categorical_columns(session_id):
        data = select_data(
            session["project_name"],
            batch="all",
            column=None,
            url=capp.config["QUALIPY_DB"],
            live_update=False,
            n_intervals=0,
            session_id=session_id,
        )
        columns = data[data.type == "categorical"].column_name.unique()
        options = [{"label": i, "value": i} for i in columns]
        return options

    @dashapp.callback(
        Output(component_id="bool-tab-col-choice", component_property="options"),
        [Input("session-id", "children")],
    )
    def bool_columns(session_id):
        data = select_data(
            session["project_name"],
            batch="all",
            column=None,
            url=capp.config["QUALIPY_DB"],
            live_update=False,
            n_intervals=0,
            session_id=session_id,
        )
        columns = data[data.type == "boolean"].column_name.unique()
        options = [{"label": i, "value": i} for i in columns]
        return options

    @dashapp.callback(
        Output(component_id="overview-page-results", component_property="children"),
        [
            Input(component_id="live-refresh", component_property="n_intervals"),
            Input(component_id="session-id", component_property="children"),
        ],
    )
    def update_tab_1(n_intervals, session_id):
        data = select_data(
            session["project_name"],
            batch="all",
            column=None,
            url=capp.config["QUALIPY_DB"],
            live_update=True,
            n_intervals=n_intervals,
            session_id=session_id,
        )
        data = data[data.type == "data-characteristic"]
        row = [
            session["project_name"],
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
        schema = html.Div(
            id="schema-table", children=[schema_title, schema_table(schema)]
        )
        anomaly_title = html.H4("Anomalies")
        print(f"session id: {session_id}")
        anom_data_session = set_session_anom_data_name(session_id)
        anom_data = get_cached_dataframe(anom_data_session)
        if anom_data is None:
            anom_data = anomaly_num_data(
                project_name=session["project_name"],
                db_url=capp.config["QUALIPY_DB"],
                config_dir=os.environ["CONFIG_DIR"],
            ).head(50)
            cache_dataframe(anom_data, anom_data_session)
        anom_table = anomaly_num_table(anom_data)
        anom_table = html.Div(id="anom-num-table", children=[anomaly_title, anom_table])
        schema_anom_div = html.Div(id="schema-anom", children=[schema, anom_table])

        overview_div = html.Div(
            id="overview-home", children=[title_data, data_char, schema_anom_div]
        )
        page = html.Div(id="home-page", children=[overview_div])
        return page

    @dashapp.callback(
        Output(component_id="numerical-page-results", component_property="children"),
        [
            Input(component_id="numerical-page-col-choice", component_property="value"),
            Input(
                component_id="numerical-page-view-choice", component_property="value"
            ),
            Input(component_id="session-id", component_property="children"),
            Input(component_id="live-refresh-num", component_property="n_intervals"),
        ],
    )
    def update_tab_2(column, view, session_id, n_intervals):
        plots = []

        if view == "detailed":
            data = select_data(
                session["project_name"],
                column=column,
                url=capp.config["QUALIPY_DB"],
                live_update=True,
                n_intervals=n_intervals,
                session_id=session_id,
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
                plot_data = set_value_type(plot_data.copy())
                plots.append(
                    html.Div(
                        id="trend-plots-{}".format(idx),
                        children=[
                            html.H3(
                                "{}-{}".format(column, metric_title, id="plot-header")
                            ),
                            create_trend_line(
                                plot_data,
                                column,
                                metric,
                                project_name=session["project_name"],
                                config_dir=os.environ["CONFIG_DIR"],
                            ),
                            histogram(plot_data, column, metric),
                        ],
                    )
                )
        elif view == "simple":
            data = select_data(
                session["project_name"],
                column=column,
                url=capp.config["QUALIPY_DB"],
                live_update=True,
                n_intervals=n_intervals,
                session_id=session_id,
            )
            data = data[data["type"] == "numerical"]
            all_trends_view = all_trends(data)
            plots.append(html.Div(children=[all_trends_view]))
        elif view == "key metrics":
            data = select_data(
                session["project_name"],
                column=None,
                url=capp.config["QUALIPY_DB"],
                live_update=True,
                n_intervals=n_intervals,
                session_id=session_id,
            )
            data = data[(data["type"] == "numerical") & (data["key_function"] == 1)]
            all_key_trends = all_trends(data, show_column_in_name=True)
            plots.append(html.Div(children=[all_key_trends]))
        return plots

    @dashapp.callback(
        Output(component_id="categorical-page-results", component_property="children"),
        [
            Input(
                component_id="categorical-page-col-choice-multi",
                component_property="value",
            ),
            Input(component_id="session-id", component_property="children"),
        ],
    )
    def update_tab_3(column, session_id):
        if column != "None":
            data = select_data(
                session["project_name"],
                column=column,
                url=capp.config["QUALIPY_DB"],
                session_id=session_id,
            )
            data = data[(data["type"] == "categorical")]
        else:
            data = pd.DataFrame({})

        if data.shape[0] > 0:
            cat_plots = []
            plot = create_value_count_area_chart(data, column)
            cat_plots.append(plot)
            bar_chart = barchart_top_cats(data)
            cat_plots.append(bar_chart)
            prop_change_plot = create_prop_change_list(data, column)
            cat_plots.append(prop_change_plot)
            cat_plots_div = html.Div(id="c-plots", children=cat_plots)

            plot_div = html.Div(
                id="cat-plots-section", children=[html.H3("Plots"), cat_plots_div]
            )
        else:
            plot_div = html.Div(id="cat-plots-section", children=[])

        return plot_div

    @dashapp.callback(
        Output(component_id="tab-4-results", component_property="children"),
        [
            Input(component_id="live-refresh-cat", component_property="n_intervals"),
            Input(component_id="session-id", component_property="children"),
        ],
    )
    def update_tab_4(n_intervals, session_id):
        data = select_data(
            session["project_name"],
            batch="all",
            column=None,
            url=capp.config["QUALIPY_DB"],
            live_update=True,
            n_intervals=n_intervals,
            session_id=session_id,
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

    @dashapp.callback(
        Output(component_id="bool-tab-results", component_property="children"),
        [
            Input(component_id="bool-tab-col-choice", component_property="value"),
            Input(component_id="live-refresh-cat", component_property="n_intervals"),
            Input(component_id="session-id", component_property="children"),
        ],
    )
    def update_tab_6(column, n_intervals, session_id):
        data = select_data(
            session["project_name"],
            batch="all",
            column=column,
            url=capp.config["QUALIPY_DB"],
            live_update=True,
            n_intervals=n_intervals,
            session_id=session_id,
        )
        error_data = select_data(
            session["project_name"],
            batch="all",
            column=None,
            url=capp.config["QUALIPY_DB"],
            live_update=True,
            n_intervals=n_intervals,
            session_id=session_id,
        )
        error_data = error_data[error_data.type == "boolean"]
        if error_data.shape[0] > 0:
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
        else:
            page = html.Div(id="tab-6-page", children=[])
        return page
