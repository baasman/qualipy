from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.serving import run_simple
import flask
from flask import request, url_for, render_template, redirect, session
from dash import Dash
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import redis

import os
import json

from web.config import Config
from web.plots.trends import create_trend_line
from web.dash_components import data_table
from web.plots.batch import compare_batch_with_rest, histogram, dot_plot
from web._layout import generate_layout



server = flask.Flask(__name__)

server.config.from_object(Config)
server.config.update(
    DEBUG=True,
    SECRET_KEY='supersecrit'
)

red = redis.Redis(host='localhost', port=6379, db=0)


HOME = os.path.expanduser('~')


dash_app1 = Dash(__name__, server=server, url_base_pathname='/metrics/')
dash_app1.config['suppress_callback_exceptions']=True
dash_app1.layout = html.Div([])

full_data = pd.DataFrame()


def select_data(project, column=None, batch=None):
    data = full_data.copy()
    if data.shape[0] == 0:
        data = pd.read_csv(os.path.join(HOME, '.qualipy/data', '{}.csv'.format(project)))
    else:
        data = full_data.copy()

    if column is not None:
        if not isinstance(column, str):
            data = data[data['_name'].isin(column)]
        else:
            data = data[data['_name'] == column]
    data = data.sort_values(['_name', '_metric', '_date'])
    if batch is None:
        return data
    else:
        if not isinstance(batch, list):
            batch = [batch]
        if 'last' not in batch:
            data = data[data['_date'].isin(batch)]
        return data


@dash_app1.callback(
    Output(component_id='tab-1-results', component_property='children'),
    [Input(component_id='column-choice', component_property='value'),
     Input(component_id='batch-choice', component_property='value')
     ]
)
def update_tab_1(column, batch):
    data = select_data(session['project'], column)
    plots = []
    plots.append(dot_plot(data, batch))
    return plots


@dash_app1.callback(
    Output(component_id='tab-2-results', component_property='children'),
    [Input(component_id='column-choice', component_property='value')]
)
def update_tab_2(column):
    plots = []
    data = select_data(session['project'], column)
    for var in data['_name'].unique():
        for metric in data[data['_name'] == var]['_metric'].unique():
            plots.append(
                html.Div(id='trend-plots',children=[
                    histogram(data, column),
                    create_trend_line(data, var, metric)
                    ]
                )
            )
    return plots


@dash_app1.callback(
    Output(component_id='tab-3-results', component_property='children'),
    [Input(component_id='column-choice', component_property='value'),
     Input(component_id='batch-choice', component_property='value')
     ]
)
def update_tab_3(column, batch):
    data = select_data(session['project'], column=column)
    plots = []
    plots.append(compare_batch_with_rest(data, batch))
    return plots


@dash_app1.callback(
    Output(component_id='tab-4-results', component_property='children'),
    [Input(component_id='column-choice', component_property='value')]
)
def update_tab_4(column):
    data = select_data(session['project'], column)
    return data_table(data)


@server.route('/', methods=['GET', 'POST'])
@server.route('/index', methods=['GET', 'POST'])
def index():
    global dash_app1
    global full_data
    with open(os.path.join(HOME, '.qualipy', 'projects.json'), 'r') as f:
        projects = json.loads(f.read())


    if request.method == 'POST':
        button_pressed = list(request.form.to_dict(flat=False).keys())[0]
        session['project'] = button_pressed
        column_options = projects[button_pressed]['columns']
        session['column_options'] = column_options

        full_data = select_data(session['project'])

        dash_app1.layout = html.Div(id='total-div',
                                    children=generate_layout(full_data, column_options))
        return redirect(url_for('render_dashboard'))
    return render_template('home/index.html', projects=list(projects.keys()))


@server.route('/metrics')
def render_dashboard():
    return flask.redirect('/dash1')


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
