import dash_core_components as dcc
import dash_table

def column_choice(column_options):
    return dcc.Dropdown(
        id='column-choice',
        options=[{'label': i, 'value': i} for i in column_options],
        value=column_options[0],
        style={
            'width': '300px',
            'marginTop': '30px'
        }
    )

def batch_choice(batches):
    return dcc.Dropdown(
        id='batch-choice',
        options=[{'label': i, 'value': i} for i in batches] + [{'label': 'all', 'value': 'all'}],
        value='all',
        multi=True,
        style={
            'width': '300px',
            'marginTop': '30px'
        }
    )

def data_table(data):
    return dash_table.DataTable(
        id='num-table',
        columns=[{'name': i, 'id': i} for i in data.columns],
        data=data.to_dict('rows'),
        sorting=True,
        style_cell={
            'textAlign': 'left',
            'minWidth': '0px', 'maxWidth': '180px',
            'whiteSpace': 'normal'
        }
    )
