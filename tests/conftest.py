from datetime import date
from pathlib import Path
from types import SimpleNamespace

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


@pytest.fixture(scope="session")
def lake(spark, tmp_path_factory):
    """Pipeline completo con dos llegadas de archivos y una publicación Gold interrumpida."""
    from lakehouse import gold, quality, silver
    from lakehouse.bronze import ingest
    from lakehouse.config import Paths
    from lakehouse.generator import write_landing
    from lakehouse.generator.catalog import LAST_ARRIVAL

    paths = Paths(str(tmp_path_factory.mktemp("lake")))
    landing = Path(paths.files) / "landing"

    write_landing(landing, until=date(2026, 10, 13))
    ingest(spark, paths)
    silver_1 = silver.build(spark, paths)
    gold_1 = gold.publish(spark, paths)
    quality_1 = quality.run(spark, paths)

    write_landing(landing, until=LAST_ARRIVAL)
    ingest(spark, paths)
    silver_2 = silver.build(spark, paths)
    with pytest.raises(gold.SimulatedFailure):
        gold.publish(spark, paths, fail_before_register=True)
    visible_after_failure = gold.current_publication(spark, paths)
    gold_2 = gold.publish(spark, paths)
    quality_2 = quality.run(spark, paths)

    silver_3 = silver.build(spark, paths)
    gold_3 = gold.publish(spark, paths)
    return SimpleNamespace(paths=paths, silver=(silver_1, silver_2, silver_3), gold=(gold_1, gold_2, gold_3),
                           quality=(quality_1, quality_2), visible_after_failure=visible_after_failure)
