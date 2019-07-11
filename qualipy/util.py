from qualipy.exceptions import InvalidColumn

import pandas as pd

import os


def get_column(data: pd.DataFrame, name: str) -> pd.Series:
    if name == "index":
        return data.index
    try:
        return data[name]
    except KeyError:
        raise InvalidColumn("Column {} is not part of the dataset".format(name))


HOME = os.path.expanduser("~")
