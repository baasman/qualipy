import dash_html_components as html
import dash_core_components as dcc
from qualipy_web.dash_components import column_choice, batch_choice, view_style


def generate_layout(
    data, numerical_column_options, standard_viz_dynamic_options, boolean_options
):

    children = []

    # General Overview
    overview_html = []
    overview_html.append(html.Br(id="placeholder"))
    overview_html.append(html.Div(id="overview-page-results"))
    overview_html.append(html.Br())
    overview_html.append(html.A("Home", href="/index", target="_blank"))
    overview_html.append(dcc.Interval(id="live-refresh", interval=10000, n_intervals=0))
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
        numerical_html.append(html.A("Home", href="/index", target="_blank"))
        numerical_html.append(
            dcc.Interval(id="live-refresh-num", interval=100000, n_intervals=0)
        )
    else:
        numerical_html.append(
            html.P("There are no numerical aggregates tracked for this dataset")
        )
    children.append(dcc.Tab(label="Numerical", value="tab-2", children=numerical_html))

    # Categorical column built-ins
    categorical_html = []
    if len(standard_viz_dynamic_options) > 0:
        categorical_html.append(html.H5("Column Choice"))
        categorical_html.append(
            column_choice(
                standard_viz_dynamic_options,
                "categorical-page-col-choice-multi",
                multi=False,
            )
        )
        categorical_html.append(html.Br())
        categorical_html.append(html.Div(id="categorical-page-results"))
        categorical_html.append(html.Br())
        categorical_html.append(html.A("Home", href="/index", target="_blank"))
    else:
        categorical_html.append(
            html.P("There are no categorical aggregates tracked for this dataset")
        )
    children.append(
        dcc.Tab(label="Categorical", value="tab-3", children=categorical_html)
    )

    # General built in data quality checks
    tab4_html = []
    tab4_html.append(
        batch_choice(data["date"].unique(), id="batch-choice-4", include_all=True)
    )
    tab4_html.append(html.Br(id="placeholder-2"))
    tab4_html.append(html.Div(id="tab-4-results"))
    tab4_html.append(html.Br())
    tab4_html.append(html.A("Home", href="/index", target="_blank"))
    tab4_html.append(dcc.Interval(id="live-refresh-cat", interval=10000, n_intervals=0))
    children.append(
        dcc.Tab(label="Data Characteristics", value="tab-4", children=tab4_html)
    )

    # Boolean variables
    boolean_tab = []
    if len(boolean_options) > 0:
        boolean_tab.append(
            column_choice(boolean_options, "bool-tab-col-choice", multi=False)
        )
        boolean_tab.append(html.Br())
        boolean_tab.append(html.Div(id="bool-tab-results"))
        boolean_tab.append(html.Br())
        boolean_tab.append(html.A("Home", href="/index", target="_blank"))
    else:
        boolean_tab.append(
            html.P("There are no boolean checks chosen for this dataset")
        )
    children.append(
        dcc.Tab(label="Boolean Metrics", value="tab-6", children=boolean_tab)
    )

    return [dcc.Tabs(id="tabs", value="tab-1", children=children)]
