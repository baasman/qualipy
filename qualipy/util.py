def get_column(data, name):
    if name == 'index':
        return data.index
    return data[name]
