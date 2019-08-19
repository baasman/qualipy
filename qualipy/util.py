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


def set_value_type(data: pd.DataFrame) -> pd.DataFrame:
    type = data.return_format.values[0]
    if type == "bool":
        data.value = data.value.map({"True": True, "False": False})
    else:
        data.value = data.value.astype(type)
    return data
