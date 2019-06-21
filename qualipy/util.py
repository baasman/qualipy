from qualipy.exceptions import InvalidColumn

import os



def get_column(data, name):
    if name == 'index':
        return data.index
    try:
        return data[name]
    except KeyError:
        raise InvalidColumn('Column {} is not part of the dataset'.format(name))


HOME = os.path.expanduser('~')
