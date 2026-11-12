import shutil
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from lakehouse import iso6346
from lakehouse.bronze import ingest
from lakehouse.config import Paths
from lakehouse.generator import write_landing
from lakehouse.generator.catalog import LAST_ARRIVAL, PERIOD_END
from lakehouse.generator.world import World
from lakehouse.silver import STATE_TABLE, build


def silver(spark, paths: Paths, table: str):
    return spark.read.format("delta").load(paths.table("silver", table))


def table_counts(spark, paths: Paths) -> dict[str, int]:
    names = ("products", "lots", "shipments", "cold_room_readings", "carrier_events", "quarantine")
    return {name: silver(spark, paths, name).count() for name in names}


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    """Dos llegadas incrementales procesadas de punta a punta."""
    paths = Paths(str(tmp_path_factory.mktemp("lake")))
    landing = Path(paths.files) / "landing"
    write_landing(landing, until=date(2026, 10, 13))
    ingest(spark, paths)
    first = build(spark, paths)
    write_landing(landing, until=LAST_ARRIVAL)
    ingest(spark, paths)
    second = build(spark, paths)
    third = build(spark, paths)
    return paths, first, second, third


def test_runs_process_only_new_bronze_batches(lake):
    _paths, first, second, third = lake
    assert first.bronze_batches == (1, 1)
    assert second.bronze_batches == (2, 2)
    assert third.bronze_batches is None


@pytest.mark.parametrize("table, keys", [
    ("lots", ["lot_code"]),
    ("shipments", ["shipment_number"]),
    ("cold_room_readings", ["room_code", "sensor_id", "reading_ts_utc"]),
    ("carrier_events", ["event_id"]),
])
def test_one_row_per_business_key(spark, lake, table, keys):
    df = silver(spark, lake[0], table)
    assert df.count() == df.select(*keys).distinct().count()


def test_local_timestamps_are_converted_to_utc(spark, lake):
    first = (silver(spark, lake[0], "cold_room_readings")
             .where("room_code = 'GT01-ESC-01' AND sensor_id = 'S1'")
             .orderBy("reading_ts_utc").first())
    assert first.reading_ts_utc == datetime(2026, 9, 1, 0, 0)
    assert first.reading_date == date(2026, 9, 1)


def test_invalid_records_go_to_quarantine_with_reason(spark, lake):
    reasons = {r.reason for r in silver(spark, lake[0], "quarantine").select("reason").distinct().collect()}
    assert {"código de error del sensor (-99.9)", "temperatura vacía", "peso no positivo",
            "producto inexistente en el maestro", "contenedor inválido según ISO 6346"} <= reasons
    readings = silver(spark, lake[0], "cold_room_readings")
    assert readings.where("temperature_c = -99.9 OR temperature_c IS NULL").count() == 0


def test_quarantine_keeps_the_original_record(spark, lake):
    record = (silver(spark, lake[0], "quarantine").where("reason = 'peso no positivo'").first().record)
    assert '"qty_kg":"-500"' in record


def test_lots_match_their_latest_version_in_the_erp(spark, lake):
    expected = {}
    for lot in World().lots:
        qty = lot.corrected_qty_kg if lot.corrected_on and lot.corrected_on <= PERIOD_END else lot.qty_kg
        expected[lot.code] = (qty, lot.shipment)
    actual = {r.lot_code: (r.qty_kg, r.shipment_number)
              for r in silver(spark, lake[0], "lots").collect()}
    assert actual == expected


def test_shipments_have_valid_containers(spark, lake):
    shipments = silver(spark, lake[0], "shipments").collect()
    assert len(shipments) == len(World().shipments) - 1          # uno en cuarentena
    assert all(iso6346.is_valid(s.container_number) for s in shipments)


def test_late_readings_are_added_by_the_second_batch(spark, lake):
    late = (silver(spark, lake[0], "cold_room_readings")
            .where("room_code = 'GT01-ESC-01' AND reading_date BETWEEN '2026-10-10' AND '2026-10-12'")
            .groupBy("_bronze_batch_id").count().collect())
    assert [r._bronze_batch_id for r in late] == [2]
    assert late[0]["count"] > 3 * 288 * 2 * 0.99                 # 3 días, 2 sensores, cada 5 minutos


def test_carrier_schema_versions_are_unified(spark, lake):
    events = silver(spark, lake[0], "carrier_events").where("event_type = 'REEFER_READING'")
    v1 = events.where("schema_version = 1")
    v2 = events.where("schema_version = 2")
    assert v1.count() > 0 and v2.count() > 0
    assert v1.where("reefer_humidity_pct IS NOT NULL").count() == 0
    assert v2.where("reefer_humidity_pct IS NULL").count() == 0


def test_reprocessing_the_same_batches_changes_nothing(spark, lake):
    paths = lake[0]
    before = table_counts(spark, paths)
    shutil.rmtree(paths.table("silver", STATE_TABLE))     # como si se hubiera perdido el avance
    replay = build(spark, paths)
    assert replay.bronze_batches == (1, 2)
    assert table_counts(spark, paths) == before
    assert sum(t.inserted for t in replay.tables.values()) == 0
