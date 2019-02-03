#numeric

def _get_mean(column):
    return column.mean()


def _get_std(column):
    return column.std()


def _get_min(column):
    return column.min()


def _get_quantile(column, quantile=.5):
    return column.quantile(quantile)


# categorical

def _get_nunique(column):
    return column.nunique()


def _get_top(column):
    return column.describe()['top']


def _get_freq(column):
    return column.describe()['freq']
