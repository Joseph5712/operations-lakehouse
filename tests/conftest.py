import pytest


@pytest.fixture(scope="session")
def spark():
    from lakehouse.spark import get_spark

    session = get_spark("tests")
    yield session
    session.stop()


@pytest.fixture
def lakehouse_root(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    return tmp_path
