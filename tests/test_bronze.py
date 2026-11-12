from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from lakehouse.bronze import REJECTED_SCHEMA, SKIPPED_DUPLICATE, SimulatedFailure, ingest
from lakehouse.config import Paths
from lakehouse.generator import write_landing
from lakehouse.generator.catalog import LAST_ARRIVAL


def bronze(spark, paths: Paths, table: str):
    return spark.read.format("delta").load(paths.table("bronze", table))


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    """Dos llegadas de archivos, con una ingesta interrumpida entre ambas."""
    paths = Paths(str(tmp_path_factory.mktemp("lake")))
    landing = Path(paths.files) / "landing"
    write_landing(landing, until=date(2026, 10, 13))
    first = ingest(spark, paths)
    write_landing(landing, until=LAST_ARRIVAL)
    with pytest.raises(SimulatedFailure):
        ingest(spark, paths, fail_before_commit=True)
    second = ingest(spark, paths)
    third = ingest(spark, paths)
    return paths, first, second, third


def test_batches_are_numbered_and_repeat_runs_do_nothing(lake):
    _paths, first, second, third = lake
    assert (first.batch_id, second.batch_id, third.batch_id) == (1, 2, None)


def test_interrupted_batch_is_cleaned_before_retry(lake):
    _paths, _first, second, _third = lake
    assert second.recovered_rows == sum(second.rows.values())


def test_every_landed_row_is_ingested_exactly_once(spark, lake):
    paths = lake[0]
    landing = Path(paths.files) / "landing"
    expected = sum(len(p.read_text().splitlines()) - 1
                   for p in landing.glob("sensors/*/*.csv") if "_retry" not in p.name)
    readings = bronze(spark, paths, "sensor_readings")
    assert readings.count() == expected
    assert readings.select("_source_file").distinct().count() == len(
        [p for p in landing.glob("sensors/*/*.csv") if "_retry" not in p.name])


def test_identical_file_with_another_name_is_skipped(spark, lake):
    log = bronze(spark, lake[0], "ingestion_log")
    retry = log.where(F.col("file_path").endswith("_retry1.csv")).first()
    assert retry.status == SKIPPED_DUPLICATE
    assert retry.duplicate_of == "sensors/arrival_date=2026-10-20/CR01-LIM-01_20261019.csv"
    assert retry.rows is None


def test_late_files_are_ingested_with_their_arrival_date(spark, lake):
    readings = bronze(spark, lake[0], "sensor_readings")
    late = (readings.where(F.col("_source_file").rlike("GT01-ESC-01_2026101[012]"))
            .select("_arrival_date", "_batch_id").distinct().collect())
    assert [(r._arrival_date, r._batch_id) for r in late] == [(date(2026, 10, 14), 2)]


def test_lineage_columns_are_complete(spark, lake):
    for table in ("erp_lots", "sensor_readings", "carrier_events"):
        df = bronze(spark, lake[0], table)
        nulls = df.where(F.col("_source_file").isNull() | F.col("_file_sha256").isNull()
                         | F.col("_batch_id").isNull() | F.col("_arrival_date").isNull()).count()
        assert nulls == 0, table


def test_carrier_payload_keeps_both_schema_versions(spark, lake):
    events = bronze(spark, lake[0], "carrier_events")
    v2 = F.col("payload").contains('"schema_version": 2')
    assert events.where((F.col("_arrival_date") < "2026-10-15") & v2).count() == 0
    assert events.where((F.col("_arrival_date") >= "2026-10-15") & ~v2).count() == 0


def test_csv_with_unexpected_header_is_rejected(spark, tmp_path):
    paths = Paths(str(tmp_path))
    folder = Path(paths.files) / "landing/sensors/arrival_date=2026-09-02"
    folder.mkdir(parents=True)
    (folder / "CR01-LIM-01_20260901.csv").write_text("room,sensor,ts,temp\nCR01-LIM-01,S1,x,1\n")
    result = ingest(spark, paths)
    assert result.files[("sensor_readings", REJECTED_SCHEMA)] == 1
    assert result.rows["sensor_readings"] == 0
