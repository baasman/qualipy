import dash_table


def overview_table(data):
    return dash_table.DataTable(
        id="overview-table",
        columns=[{"name": i, "id": i} for i in data.columns],
        data=data.to_dict("rows"),
        sort_action="native",
    )


def schema_table(data):
    return dash_table.DataTable(
        id="schema-table",
        columns=[{"name": i, "id": i} for i in data.columns],
        data=data.to_dict("rows"),
        sort_action="native",
    )
