import dash_html_components as html
import dash_core_components as dcc
from web.dash_components import column_choice, batch_choice


def generate_layout(data, column_options):
    batch_choices = batch_choice(data['_date'].unique())

    tab1_html = []
    tab1_html.append(html.H5('Batch Choice'))
    tab1_html.append(batch_choices)
    tab1_html.append(html.Br())
    tab1_html.append(html.Div(id='tab-1-results'))
    tab1_html.append(html.Br())
    tab1_html.append(html.A('Home', href='/index', target='_blank'))

    tab2_html = []
    tab2_html.append(html.H5('Column Choice'))
    tab2_html.append(column_choice(column_options, 'tab-2-col-choice', multi=False))
    tab2_html.append(html.Br())
    tab2_html.append(html.Div(id='tab-2-results'))
    tab2_html.append(html.Br())
    tab2_html.append(html.A('Home', href='/index', target='_blank'))

    return [
        html.Img(src='/assets/logo.png', style={'width': '300px', 'height': 'auto'}),
        dcc.Tabs(id="tabs", value='tab-1', children=[
            dcc.Tab(
                label='Overview',
                value='tab-1',
                children=tab1_html
            ),
            dcc.Tab(
                label='Trends',
                value='tab-2',
                children=tab2_html
            )
        ]),
    ]
