from qualipy import DataSet, Project, Column
from qualipy.backends.pandas_backend.pandas_types import (
    IntType,
    FloatType,
    DateTimeType,
)
from qualipy.exceptions import InvalidType, NullableError

import pytest
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

import os


@pytest.fixture
def data():
    data = pd.DataFrame(
        {
            "integer_col": [1, 2, 3],
            "float_col": [1.0, 2.0, 3.0],
            "date_col": ["2018/01/01", "2018/01/02", "2018/01/03"],
        }
    )
    data.date_col = pd.to_datetime(data.date_col)
    return data


@pytest.fixture
def db():
    return create_engine(
        "sqlite:///".format(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "test.db")
        )
    )


@pytest.fixture
def project(db):
    return Project(project_name="test", engine=db, reset=True)


######## type checking #########

def test_type_check(data, project):
    class IntCheck(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(IntCheck())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    assert 1


def test_type_check_fail(data, project):
    class IntCheck(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(IntCheck())

    data.integer_col = data.integer_col.astype(float)
    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    with pytest.raises(InvalidType):
        ds.run()


######## null checking #########

def test_null_check(data, project):
    class NullCheck(Column):
        column_name = "integer_col"
        column_type = IntType()
        null = False
        force_null = True

    project.add_column(NullCheck())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    assert 1


def test_null_check_fail(data, project):
    class NullCheck(Column):
        column_name = "float_col"
        column_type = FloatType()
        null = False
        force_null = True

    project.add_column(NullCheck())

    data.loc[0, 'float_col'] = np.NaN
    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    with pytest.raises(NullableError):
        ds.run()
