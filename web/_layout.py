import dash_html_components as html
import dash_core_components as dcc
from web.dash_components import column_choice, batch_choice, data_table


def generate_layout(data, column_options):
    col_choices = column_choice(column_options)
    batch_choices = batch_choice(data['_date'].unique())

    tab1_html = []
    tab1_html.append(html.H3('History'))
    tab1_html.append(col_choices)
    tab1_html.append(html.Div(id='tab-1-results'))

    tab2_html = []
    tab2_html.append(html.H3('Trends'))
    tab2_html.append(col_choices)
    tab2_html.append(html.Div(id='tab-2-results'))

    tab3_html = []
    tab3_html.append(html.H3('Analyze a batch'))
    tab3_html.append(batch_choices)
    tab2_html.append(col_choices)
    tab3_html.append(html.Div(id='tab-3-results'))

    return [
        html.Img(src='/assets/logo.png', style={'width': '300px', 'height': 'auto'}),
        dcc.Tabs(id="tabs", value='tab-1', children=[
            dcc.Tab(
                label='Data',
                value='tab-1',
                children=tab1_html
            ),
            dcc.Tab(
                label='Trends',
                value='tab-2',
                children=tab2_html
            ),
            dcc.Tab(
                label='Batch',
                value='tab-3',
                children=tab3_html
            ),
        ]),
    ]
