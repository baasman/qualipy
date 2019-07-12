from qualipy import DataSet, Project, Column
from qualipy.backends.pandas_backend.pandas_types import (
    IntType,
    FloatType,
    DateTimeType,
    NumericType,
)
from qualipy.backends.pandas_backend.functions import mean, number_of_outliers
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
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test.db")
    engine = create_engine("sqlite:///".format(path))
    yield engine
    try:
        os.remove(path)
    except:
        pass


@pytest.fixture
def project(db):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test.db")
    return Project(project_name="test", engine=db, reset_config=True)


####### project checks ##########


def test_project_get_table(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data.shape[0] > 0


def test_default_checks_done(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data[hist_data.metric == "perc_missing"].value.values[0] == 0
    assert (
        hist_data[
            (hist_data.metric == "count") & (hist_data.column_name == "rows")
        ].value.values[0]
        == 3
    )
    assert (
        hist_data[
            (hist_data.metric == "count") & (hist_data.column_name == "columns")
        ].value.values[0]
        == 3
    )
    assert hist_data[hist_data.metric == "dtype"].value.values[0] == "int64"


def test_delete_data(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data.shape[0] > 0

    project.delete_data()

    hist_data = project.get_project_table()
    assert hist_data.shape[0] == 0


######## type checking #########


def test_type_check(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    assert 1


def test_type_check_fail(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    data.integer_col = data.integer_col.astype(float)
    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    with pytest.raises(InvalidType):
        ds.run()


######## null checking #########


def test_null_check(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        null = False
        force_null = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    assert 1


def test_null_check_fail(data, project):
    class TestCol(Column):
        column_name = "float_col"
        column_type = FloatType()
        null = False
        force_null = True

    project.add_column(TestCol())

    data.loc[0, "float_col"] = np.NaN
    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    with pytest.raises(NullableError):
        ds.run()


############# functions ##############


def test_function_call_without_arguments(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        null = False
        force_null = True
        functions = [mean]

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data[hist_data.metric == "mean"].value.values[0] == 2.0


def test_function_call_without_arguments_dict_style(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        null = False
        force_null = True
        functions = [{"function": mean}]

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data[hist_data.metric == "mean"].value.values[0] == 2.0


def test_function_call_with_arguments(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        null = False
        force_null = True
        functions = [{"function": number_of_outliers, "parameters": {"std_away": 2}}]

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data[hist_data.metric == "number_of_outliers"].value.values[0] == 0


###### multiple columns #############


def test_multiple_columns(data, project):
    class TestCol(Column):
        column_name = ["integer_col", "float_col"]
        column_type = NumericType()
        null = False
        force_null = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert "integer_col" in hist_data.column_name.values
    assert "float_col" in hist_data.column_name.values
