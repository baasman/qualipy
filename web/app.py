from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.serving import run_simple
import flask
from flask import Response, request, abort, url_for, render_template, redirect, session
from dash import Dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output

import os
import json

import dash_table
import pandas as pd


server = flask.Flask(__name__)

# server.config.from_object(app_config['development'])
server.config.update(
    DEBUG=True,
    SECRET_KEY='secret_xxx'
)


HOME = os.path.expanduser('~')


@server.errorhandler(401)
def page_not_found(e):
    print(e)
    return Response('<p>Page not found</p>')


dash_app1 = Dash(__name__, server=server, url_base_pathname='/metrics/')
dash_app1.layout = html.Div([
    html.H1('Qualipy'),
    dcc.Tabs(id="tabs", value='tab-1', children=[
        dcc.Tab(label='History', value='tab-1'),
        dcc.Tab(label='Numeric', value='tab-2'),
        dcc.Tab(label='Categorical', value='tab-3'),
    ]),
    html.Div(id='index')
])
@dash_app1.callback(Output('index', 'children'),
                    [Input('tabs', 'value')])
def render_content(tab):
    print(session['project'])
    if tab == 'tab-1':
        return html.Div([
            html.H3('History'),
        ])
    elif tab == 'tab-2':
        return html.Div([
            html.H3('Numerical Metrics'),
        ])
    elif tab == 'tab-3':
        return html.Div([
            html.H3('Categorical Metrics'),
        ])


@server.route('/', methods=['GET', 'POST'])
@server.route('/index', methods=['GET', 'POST'])
def index():
    with open(os.path.join(HOME, '.qualipy', 'projects.json'), 'r') as f:
        projects = json.loads(f.read())

    if request.method == 'POST':
        button_pressed = list(request.form.to_dict(flat=False).keys())[0]
        session['project'] = button_pressed
        return redirect(url_for('render_dashboard'))
    return render_template('home/index.html', projects=list(projects.keys()))


@server.route('/metrics')
def render_dashboard():
    print('hello')
    return flask.redirect('/dash1')


app = DispatcherMiddleware(server, {
    '/dash1': dash_app1.server,
})

if __name__ == '__main__':
    run_simple('localhost', 5005, app, use_reloader=True, use_debugger=True)
