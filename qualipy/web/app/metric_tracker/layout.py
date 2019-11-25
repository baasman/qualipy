import dash_html_components as html
import dash_core_components as dcc
from qualipy.web.app.metric_tracker.util_components import (
    column_choice,
    batch_choice,
    view_style,
)

from typing import List
import uuid


def generate_layout(
    numerical_column_options: List[str],
    categorical_column_options: List[str],
    boolean_options: List[str],
    interval_time: int,
):

    session_id = str(uuid.uuid4())

    children = []

    # General Overview
    overview_html = []
    overview_html.append(
        html.Div(session_id, id="session-id", style={"display": "none"})
    )
    overview_html.append(html.Div(id="none", children=[], style={"display": "none"}))
    overview_html.append(html.Div(id="overview-page-results"))
    overview_html.append(html.Br())
    overview_html.append(html.A("Home", href="/", target="_blank"))
    overview_html.append(
        dcc.Interval(id="live-refresh", interval=interval_time, n_intervals=0)
    )
    children.append(dcc.Tab(label="Overview", value="tab-1", children=overview_html))

    # Numerical aggregate trends
    numerical_html = []
    if len(numerical_column_options) > 0:
        numerical_html.append(html.H5("Column Choice"))
        numerical_html.append(
            column_choice(
                numerical_column_options, "numerical-page-col-choice", multi=False
            )
        )
        numerical_html.append(html.H5("Type of view"))
        numerical_html.append(view_style("numerical-page-view-choice", multi=False))
        numerical_html.append(html.Br())
        numerical_html.append(html.Div(id="numerical-page-results"))
        numerical_html.append(html.Br())
        numerical_html.append(html.A("Home", href="/", target="_blank"))
        numerical_html.append(
            dcc.Interval(id="live-refresh-num", interval=interval_time, n_intervals=0)
        )
    else:
        numerical_html.append(
            html.P("There are no numerical aggregates tracked for this dataset")
        )
    children.append(dcc.Tab(label="Numerical", value="tab-2", children=numerical_html))
    #
    # Categorical column built-ins
    categorical_html = []
    if len(categorical_column_options) > 0:
        categorical_html.append(html.H5("Column Choice"))
        categorical_html.append(
            column_choice(
                categorical_column_options,
                "categorical-page-col-choice-multi",
                multi=False,
            )
        )
        categorical_html.append(html.Br())
        categorical_html.append(html.Div(id="categorical-page-results"))
        categorical_html.append(html.Br())
        categorical_html.append(html.A("Home", href="/", target="_blank"))
    else:
        categorical_html.append(
            html.P("There are no categorical aggregates tracked for this dataset")
        )
    children.append(
        dcc.Tab(label="Categorical", value="tab-3", children=categorical_html)
    )
    #
    # # General built in data quality checks
    tab4_html = []
    tab4_html.append(batch_choice([], id="batch-choice-4", include_all=True))
    tab4_html.append(html.Br(id="placeholder-2"))
    tab4_html.append(html.Div(id="tab-4-results"))
    tab4_html.append(html.Br())
    tab4_html.append(html.A("Home", href="/", target="_blank"))
    tab4_html.append(
        dcc.Interval(id="live-refresh-cat", interval=interval_time, n_intervals=0)
    )
    children.append(
        dcc.Tab(label="Data Characteristics", value="tab-4", children=tab4_html)
    )
    #
    # # Boolean variables
    boolean_tab = []
    if len(boolean_options) > 0:
        boolean_tab.append(
            column_choice(boolean_options, "bool-tab-col-choice", multi=False)
        )
        boolean_tab.append(html.Br())
        boolean_tab.append(html.Div(id="bool-tab-results"))
        boolean_tab.append(html.Br())
        boolean_tab.append(html.A("Home", href="/", target="_blank"))
    else:
        boolean_tab.append(
            html.P("There are no boolean checks chosen for this dataset")
        )
    children.append(
        dcc.Tab(label="Boolean Metrics", value="tab-6", children=boolean_tab)
    )

    return html.Div(
        id="total-div", children=[dcc.Tabs(id="tabs", value="tab-1", children=children)]
    )