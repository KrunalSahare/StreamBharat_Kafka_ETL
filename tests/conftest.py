"""Shared pytest fixtures. Run tests inside the workspace container:  pytest -q"""
import pytest

from sbetl.common.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests", master="local[2]", extra_conf={"spark.sql.shuffle.partitions": "2",
                                                         "spark.ui.enabled": "false"})
    yield s
    s.stop()
