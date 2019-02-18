from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.serving import run_simple
import flask
from flask import Response, request, abort, url_for, render_template, redirect, session
from dash import Dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output, State
import numpy as np
import dash_table
import pandas as pd

import os
import json

from web.config import Config
from web.plots.trends import create_trend_line



server = flask.Flask(__name__)

server.config.from_object(Config)
server.config.update(
    DEBUG=True,
    SECRET_KEY='supersecrit'
)


HOME = os.path.expanduser('~')


@server.errorhandler(401)
def page_not_found(e):
    print(e)
    return Response('<p>Page not found</p>')


dash_app1 = Dash(__name__, server=server, url_base_pathname='/metrics/')
dash_app1.config['suppress_callback_exceptions']=True
dash_app1.layout = html.Div([])


@dash_app1.callback(Output('index', 'children'),
                    [Input('tabs', 'value'), Input('column-choice', 'value')])
def render_content(tab, value):
    cat_data = pd.read_csv(os.path.join(HOME, '.qualipy/data', '{}-cat.csv'.format(session['project'])))
    cat_data = cat_data[cat_data['_name'] == value]
    cat_data = cat_data.sort_values(['_name', '_metric', '_date'])
    num_data = pd.read_csv(os.path.join(HOME, '.qualipy/data', '{}-num.csv'.format(session['project'])))
    num_data = num_data[num_data['_name'] == value]
    num_data = num_data.sort_values(['_name', '_metric', '_date'])
    data = pd.concat([num_data, cat_data])
    if tab == 'tab-1':
        return html.Div([
            html.H3('History'),
            html.H5('Data'),
            dash_table.DataTable(
                id='num-table',
                columns=[{'name': i, 'id': i} for i in data.columns],
                data=data.to_dict('rows'),
                sorting=True,
                style_cell={
                    'textAlign': 'left',
                    'minWidth': '0px', 'maxWidth': '180px',
                    'whiteSpace': 'normal'
                }
            ),
        ])
    elif tab == 'tab-2':
        final_html = []
        final_html.append(html.H3('Trends'))
        for var in data['_name'].unique():
            for metric in data[data['_name'] == var]['_metric'].unique():
                final_html.append(
                    create_trend_line(data, var, metric)
                )
        return final_html
    elif tab == 'tab-3':
        final_html = []
        final_html.append(html.H3('Analyze a batch'))
        return final_html


@server.route('/', methods=['GET', 'POST'])
@server.route('/index', methods=['GET', 'POST'])
def index():
    global dash_app1
    with open(os.path.join(HOME, '.qualipy', 'projects.json'), 'r') as f:
        projects = json.loads(f.read())


    if request.method == 'POST':
        button_pressed = list(request.form.to_dict(flat=False).keys())[0]
        session['project'] = button_pressed
        column_options = projects[button_pressed]['columns']
        dash_app1.layout = html.Div([
            html.Img(src='/assets/logo.png', style={'width': '300px', 'height': 'auto'}),
            dcc.Tabs(id="tabs", value='tab-1', children=[
                dcc.Tab(label='History', value='tab-1'),
                dcc.Tab(label='Trends', value='tab-2'),
                dcc.Tab(label='Batch', value='tab-3'),
            ]),
            dcc.Dropdown(
                id='column-choice',
                options=[{'label': i, 'value': i} for i in column_options],
                value=column_options[0],
                style={
                    'width': '300px',
                    'marginTop': '30px'
                }
            ),
            html.Div(
                id='index',
            )
        ])
        return redirect(url_for('render_dashboard'))
    return render_template('home/index.html', projects=list(projects.keys()))


@server.route('/metrics')
def render_dashboard():
    return flask.redirect('/dash1')


app = DispatcherMiddleware(server, {
    '/dash1': dash_app1.server,
})

if __name__ == '__main__':
    run_simple('localhost', 5005, app, use_reloader=True, use_debugger=True)
