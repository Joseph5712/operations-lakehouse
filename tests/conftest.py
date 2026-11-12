import pytest

from lakehouse.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests")
    yield session
    session.stop()


@pytest.fixture
def lakehouse_root(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    return tmp_path
