from werkzeug.wsgi import DispatcherMiddleware
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
import pickle

from web.config import Config
from web.plots.trends import (
    create_trend_line,
    create_value_count_area_chart,
    histogram
)
from web.plots.overview import (
    create_type_plots,
    create_simple_line_plot_subplots,
    create_unique_columns_plot,
    bar_plot_missing
)
from web.plots.batch import (
    heatmap
)
from web.dash_components import overview_table, schema_table, alerts_markdown
from web.layout import generate_layout
from qualipy.database import get_table
from qualipy.run import STANDARD_VIZ



server = flask.Flask(__name__)
load_dotenv()

server.config.from_object(Config)
server.config.update(
    DEBUG=True,
    SECRET_KEY='supersecrit'
)
config = Config()


HOME = os.path.expanduser('~')


dash_app1 = Dash(__name__, server=server, url_base_pathname='/metrics/')
dash_app1.config['suppress_callback_exceptions']=True
dash_app1.layout = html.Div([])

full_data = pd.DataFrame()
alert_data = pd.DataFrame()


def select_data(project, column=None, batch=None, url=None, general=False):
    global full_data
    data = full_data.copy()

    try:
        if data.shape[0] == 0 or general:
            engine = create_engine(url)
            data = get_table(engine, project)
            data.value = data.value.apply(lambda r: pickle.loads(r))
        else:
            data = full_data.copy()
    except:
        raise Exception("Can't find any data at {}".format(url))

    if column is not None and not general:
        if not isinstance(column, str):
            data = data[data['_name'].isin(column)]
        else:
            data = data[data['_name'] == column]
    data = data.sort_values(['_name', '_metric', '_date'])

    if general:
        data = data[data['_type'] == 'overview']
    else:
        data = data[data['_type'] != 'overview']

    if batch is None or batch == 'all':
        return data
    else:
        if not isinstance(batch, list):
            batch = [batch]
        try:
            batch_values = [i['value'] for i in batch]
        except TypeError:
            batch_values = batch
        data = data[data['_date'].isin(batch_values)]
        return data


def get_alerts_table(project, url):
    global alert_data

    data = alert_data.copy()
    engine = create_engine(url)
    if data.shape[0] == 0:
        data = get_table(engine, '{}_alerts'.format(project))
        alert_data = data.copy()

    data = data.sort_values('date', ascending=False)
    return data


#### Overview Tab ####

@dash_app1.callback(
    Output(component_id='tab-1-results', component_property='children'),
    [Input(component_id='placeholder', component_property='n_clicks')]
)
def update_tab_1(n_clicks):
    data = select_data(session['project'],
                       batch='all',
                       column=None,
                       url=session['db_url'],
                       general=True)
    row = [
        session['project'],
        data[data['_name'] == 'rows'].value.sum(),
        data[data['_name'] == 'columns'].value.iloc[-1],
        data['_date'].min(),
        data['_date'].nunique(),
    ]
    over_table = pd.DataFrame([row],
                              columns=['project', 'number_of_rows',
                                       'number_of_columns', 'last_run_time',
                                       'number_of_batches'])
    schema = pd.DataFrame([{'column': col, 'type': info['dtype'], 'null': info['nullable'],
                            'unique': info['unique']} for
                           col, info in session['schema'].items()])
    schema['null'] = schema['null'].astype(str)
    schema['unique'] = schema['unique'].astype(str)

    alerts = get_alerts_table(session['project'], url=session['db_url'])
    markdown = alerts_markdown(alerts.head(10))

    title_data = html.H4('Data characteristics')
    data_char = overview_table(over_table)
    schema_title = html.H4('Schema')
    schema = schema_table(schema)

    overview_div = html.Div(id='overview-home',
                        children=[
                            title_data, data_char, schema_title, schema
                        ])
    alerts_div = html.Div(id='alerts-home',
                        children=[
                            markdown
                        ])
    page = html.Div(id='home-page',
                    children=[
                        overview_div, alerts_div
                    ])
    return page


#### Numerical Trends ####

@dash_app1.callback(
    Output(component_id='tab-2-results', component_property='children'),
    [Input(component_id='tab-2-col-choice', component_property='value')]
)
def update_tab_2(column):
    plots = []
    data = select_data(session['project'], column=column,
                       url=session['db_url'])
    data = data[data['_type'] == 'custom']

    for idx, metric in enumerate(data[data['_name'] == column]['_metric'].unique()):
        args = data[data['_metric'] == metric]['_arguments'].iloc[0]
        metric_title = metric if args is None else '{}_{}'.format(metric, args)
        plot_data = data[(data['_name'] == column) &
                         (data['_metric'] == metric)]
        plots.append(
            html.Div(id='trend-plots-{}'.format(idx),
                     children=[
                         html.H3('{}-{}'.format(column, metric_title, id='plot-header')),
                         create_trend_line(plot_data, column, metric),
                         histogram(plot_data, column, metric)
                     ]
            )
        )
    return plots


#### Categorical Trends ####

@dash_app1.callback(
    Output(component_id='tab-3-results', component_property='children'),
    [Input(component_id='tab-3-col-choice-multi', component_property='value')]
)
def update_tab_3(column):
    data = select_data(session['project'], column=column,
                       url=session['db_url'])

    data = data[(data['_type'] == 'standard_viz') & (data['_over_time'] == True)]

    if data[data['_standard_viz'] == 'value_counts'].shape[0] > 0:
        value_count_plots = []
        for col in data['_name'].unique():
            plot_data = data[(data['_name'] == column) &
                             (data['_standard_viz'] == 'value_counts')]
            value_count_plots.append(create_value_count_area_chart(plot_data, col, 'value_counts'))
        value_count_plots = html.Div(id='value-count-plots',
                                   children=value_count_plots)
        value_count_div = html.Div(
            id='value-count-section',
            children=[
                html.H3('Value Frequencies'),
                value_count_plots
            ]
        )
    else:
        value_count_div = html.Div(
            id='value-count-section',
            children=[]
        )
    return value_count_div


#### Data Characteristics tab ####

@dash_app1.callback(
    Output(component_id='tab-4-results', component_property='children'),
    [Input(component_id='batch-choice-4', component_property='value')]
)
def update_tab_4(batch):
    data = select_data(session['project'], column=None, batch=batch,
                       url=session['db_url'], general=True)
    type_plot = create_type_plots(data[(data['_metric'] == 'dtype')].copy(), session['schema'])
    missing_plot = bar_plot_missing(data[(data['_metric'] == 'perc_missing')].copy(), session['schema'])
    rows_columns = create_simple_line_plot_subplots(data.copy())
    unique_plot = create_unique_columns_plot(data[(data['_metric'] == 'is_unique')].copy())

    ###### row 1
    line_plots = html.Div(id='data-line-plots',
                          children=[
                              rows_columns
                          ])

    missing = html.Div(id='missing',
                          children=[
                              missing_plot
                          ])
    row1 = html.Div(id='missing-and-rows-column',
                     children=[
                         line_plots,
                         missing
                     ])
    ###### row 2
    data_type_plots = html.Div(id='data-type-plots',
                               children=[
                                   type_plot
                               ])
    unique_plot = html.Div(id='unique-plot',
                               children=[
                                   unique_plot
                               ])
    row2 = html.Div(id='data-type-and-unique-row',
                    children=[
                        data_type_plots,
                        unique_plot
                    ])



    page = html.Div(id='overview-page',
                    children=[
                        row1,
                        row2
                    ])
    return page


#### Single Batch analyzer tab ####

@dash_app1.callback(
    Output(component_id='tab-5-results', component_property='children'),
    [Input(component_id='batch-choice-5', component_property='value'),
     Input(component_id='tab-5-col-choice', component_property='value')]
)
def update_tab_5(batch, column):
    data = select_data(session['project'], column=column,
                       batch=batch, url=session['db_url'])

    data = data[(data['_type'] == 'standard_viz') & (data['_over_time'] == False)]

    heatmap_plots = []
    for metric in data['_metric'].unique():
        plot_data = data[(data['_name'] == column) & (data['_metric'] == metric)]
        heatmap_plots.append(heatmap(plot_data, column, metric))

    page = html.Div(id='tab-5-page',
                    children=heatmap_plots)
    return page

@server.route('/', methods=['GET', 'POST'])
@server.route('/index', methods=['GET', 'POST'])
def index():
    global dash_app1
    global full_data

    session.clear()

    project_file = os.getenv('PROJECT_FILE', os.path.join(HOME, '.qualipy', 'projects.json'))

    with open(project_file, 'r') as f:
        projects = json.loads(f.read())

    if request.method == 'POST':
        button_pressed = list(request.form.to_dict(flat=False).keys())[0]
        session['project'] = button_pressed
        column_options = projects[button_pressed]['columns']
        session['column_options'] = column_options
        if hasattr(config, 'DB_URL'):
            url = config.DB_URL
        else:
            url = projects[button_pressed].get('db')
        session['db_url'] = url
        session['schema'] = projects[button_pressed]['schema']

        full_data = select_data(session['project'], url=url)

        try:
            standard_over_time = full_data[(full_data['_over_time'] == True) &
                                           (full_data['_type'] == 'standard_viz')]['_name'].unique()
            standard_not_over_time = full_data[(full_data['_over_time'] == False) &
                                               (full_data['_type'] == 'standard_viz')]['_name'].unique()
        except:
            standard_over_time = []
            standard_not_over_time = []


        dash_app1.layout = html.Div(id='total-div',
                                    children=generate_layout(data=full_data, column_options=column_options,
                                                             standard_over_time=standard_over_time,
                                                             standard_not_over_time=standard_not_over_time))
        return redirect(url_for('render_dashboard'))
    return render_template('home/index.html', projects=projects)


@server.route('/metrics')
def render_dashboard():
    return redirect('/dash1')


app = DispatcherMiddleware(server, {
    '/dash1': dash_app1.server,
})

dash_app1.css.append_css({
    'external_url': (
	'https://github.com/plotly/dash-app-stylesheets/blob/master/dash-analytics-report.css'
    )
})

if __name__ == '__main__':
    run_simple('localhost', 5005, app, use_reloader=True, use_debugger=True)
