import tempfile
import shutil
import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def tmp_dir():
    """
    Create a temp directory and return a path.
    At the end of each function, remove the temp directory.
    """

    tmpdir = Path(tempfile.mkdtemp())

    yield tmpdir

    shutil.rmtree(tmpdir, ignore_errors=True)

    return tmpdir