import dash_core_components as dcc
import dash_table

def column_choice(column_options, id='column-choice', multi=True):
    return dcc.Dropdown(
        id=id,
        options=[{'label': i, 'value': i} for i in column_options],
        value=column_options[0],
        multi=multi,
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

def overview_table(data):
    return dash_table.DataTable(
        id='overview-table',
        columns=[{'name': i, 'id': i} for i in data.columns],
        data=data.to_dict('rows'),
        sorting=True,
    )

def schema_table(data):
    return dash_table.DataTable(
        id='schema-table',
        columns=[{'name': i, 'id': i} for i in data.columns],
        data=data.to_dict('rows'),
        sorting=True,
    )
