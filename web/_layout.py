import dash_html_components as html
import dash_core_components as dcc
from web.dash_components import column_choice, batch_choice


def generate_layout(data, column_options, value_count_column_options):


    # General Overview
    tab1_html = []
    tab1_html.append(html.Br(id='placeholder'))
    tab1_html.append(html.Div(id='tab-1-results'))
    tab1_html.append(html.Br())
    tab1_html.append(html.A('Home', href='/index', target='_blank'))


    # Numerical aggregate trends
    tab2_html = []
    tab2_html.append(html.H5('Column Choice'))
    tab2_html.append(column_choice(column_options, 'tab-2-col-choice', multi=False))
    tab2_html.append(html.Br())
    tab2_html.append(html.Div(id='tab-2-results'))
    tab2_html.append(html.Br())
    tab2_html.append(html.A('Home', href='/index', target='_blank'))


    # Categorical column built-ins
    tab3_html = []
    tab3_html.append(html.H5('Column Choice'))
    tab3_html.append(column_choice(value_count_column_options, 'tab-3-col-choice', multi=True))
    tab3_html.append(html.Br())
    tab3_html.append(html.Div(id='tab-3-results'))
    tab3_html.append(html.Br())
    tab3_html.append(html.A('Home', href='/index', target='_blank'))


    # General built in data quality checks
    tab4_html = []
    tab4_html.append(batch_choice(data['_date'].unique(), id='batch-choice-4', include_all=True))
    tab4_html.append(html.Br(id='placeholder-2'))
    tab4_html.append(html.Div(id='tab-4-results'))
    tab4_html.append(html.Br())
    tab4_html.append(html.A('Home', href='/index', target='_blank'))


    # Single batch analyzer
    tab5_html = []
    tab5_html.append(batch_choice(data['_date'].unique(), id='batch-choice-5',
                                  include_all=False, multi=False))
    tab5_html.append(html.Br())
    tab5_html.append(html.Div(id='tab-5-results'))
    tab5_html.append(html.Br())
    tab5_html.append(html.A('Home', href='/index', target='_blank'))

    return [
        # html.Img(src='/assets/logo.png', style={'width': '300px', 'height': 'auto'}),
        dcc.Tabs(id="tabs", value='tab-1', children=[
            dcc.Tab(
                label='Overview',
                value='tab-1',
                children=tab1_html
            ),
            dcc.Tab(
                label='Numerical',
                value='tab-2',
                children=tab2_html
            ),
            dcc.Tab(
                label='Categorical',
                value='tab-3',
                children=tab3_html
            ),
            dcc.Tab(
                label='Data Characteristics',
                value='tab-4',
                children=tab4_html
            ),
            dcc.Tab(
                label='Single Batch Metrics',
                value='tab-5',
                children=tab5_html
            )
        ]),
    ]
