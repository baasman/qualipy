import dash_html_components as html
import dash_core_components as dcc
from web.dash_components import column_choice, batch_choice


def generate_layout(data, column_options, standard_over_time, standard_not_over_time):

    children = []

    # General Overview
    tab1_html = []
    tab1_html.append(html.Br(id='placeholder'))
    tab1_html.append(html.Div(id='tab-1-results'))
    tab1_html.append(html.Br())
    tab1_html.append(html.A('Home', href='/index', target='_blank'))
    children.append(dcc.Tab(
        label='Overview',
        value='tab-1',
        children=tab1_html
    ))


    # Numerical aggregate trends
    tab2_html = []
    tab2_html.append(html.H5('Column Choice'))
    tab2_html.append(column_choice(column_options, 'tab-2-col-choice', multi=False))
    tab2_html.append(html.Br())
    tab2_html.append(html.Div(id='tab-2-results'))
    tab2_html.append(html.Br())
    tab2_html.append(html.A('Home', href='/index', target='_blank'))
    children.append(dcc.Tab(
        label='Numerical',
        value='tab-2',
        children=tab2_html
    ))


    # Categorical column built-ins
    tab3_html = []
    tab3_html.append(html.H5('Column Choice'))
    tab3_html.append(column_choice(standard_over_time, 'tab-3-col-choice-multi', multi=True))
    tab3_html.append(html.Br())
    tab3_html.append(html.Div(id='tab-3-results'))
    tab3_html.append(html.Br())
    tab3_html.append(html.A('Home', href='/index', target='_blank'))
    children.append(dcc.Tab(
        label='Categorical',
        value='tab-3',
        children=tab3_html
    ))


    # General built in data quality checks
    tab4_html = []
    tab4_html.append(batch_choice(data['_date'].unique(), id='batch-choice-4', include_all=True))
    tab4_html.append(html.Br(id='placeholder-2'))
    tab4_html.append(html.Div(id='tab-4-results'))
    tab4_html.append(html.Br())
    tab4_html.append(html.A('Home', href='/index', target='_blank'))
    children.append(dcc.Tab(
        label='Data Characteristics',
        value='tab-4',
        children=tab4_html
    ))


    # Single batch analyzer
    tab5_html = []
    if len(standard_not_over_time) > 0:
        tab5_html.append(batch_choice(data['_date'].unique(), id='batch-choice-5',
                                      include_all=False, multi=False))
        tab5_html.append(column_choice(standard_not_over_time, 'tab-5-col-choice', multi=False))
        tab5_html.append(html.Br())
        tab5_html.append(html.Div(id='tab-5-results'))
        tab5_html.append(html.Br())
        tab5_html.append(html.A('Home', href='/index', target='_blank'))
        children.append(dcc.Tab(
            label='Single Batch Metrics',
            value='tab-5',
            children=tab5_html
        ))


    return [
        # html.Img(src='/assets/logo.png', style={'width': '300px', 'height': 'auto'}),
        dcc.Tabs(id="tabs", value='tab-1', children=children),
    ]
