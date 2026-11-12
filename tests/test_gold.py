from datetime import date, datetime

from pyspark.sql import functions as F

from lakehouse.gold import CRITICAL_DURATION_MIN, EXCURSION_MIN_DURATION_MIN, current
from lakehouse.generator.world import COMPRESSOR_FAILURE, World


def gold(spark, lake, table):
    return current(spark, lake.paths, table)


def published(spark, lake, table, publication_id):
    df = spark.read.format("delta").load(lake.paths.table("gold", table))
    return df.where(F.col("_publication_id") == publication_id)


def test_publications_are_numbered_and_skipped_when_nothing_changed(lake):
    first, second, third = lake.gold
    assert (first.publication_id, second.publication_id) == (1, 2)
    assert third.skipped and third.publication_id == 2


def test_interrupted_publication_is_never_visible(lake):
    assert lake.visible_after_failure == 1
    assert lake.gold[1].removed_orphan_rows > 0


def test_current_and_previous_publications_are_kept(spark, lake):
    ids = {r[0] for r in spark.read.format("delta").load(lake.paths.table("gold", "fact_lot_cold_chain"))
           .select("_publication_id").distinct().collect()}
    assert ids == {1, 2}


def test_compressor_failure_is_a_critical_excursion(spark, lake):
    failure = (gold(spark, lake, "fact_cold_room_excursion")
               .where(f"room_code = '{COMPRESSOR_FAILURE['room']}' AND severity = 'CRITICA'").collect())
    assert len(failure) == 1
    e = failure[0]
    assert COMPRESSOR_FAILURE["start"].replace(tzinfo=None) <= e.start_utc < datetime(2026, 10, 7, 16, 0)
    assert e.duration_min >= 4 * 60 and e.peak_temp_c > 17


def test_short_episodes_are_not_excursions(spark, lake):
    excursions = gold(spark, lake, "fact_cold_room_excursion")
    assert excursions.where(f"duration_min < {EXCURSION_MIN_DURATION_MIN}").count() == 0
    # Tres descongelamientos diarios en dos cuartos de congelado no generan excursiones.
    assert excursions.where("temp_class = 'FROZEN'").count() < 10
    assert excursions.where(f"severity = 'CRITICA' AND duration_min < {CRITICAL_DURATION_MIN}").count() == 0


def test_lots_in_the_failed_room_are_compromised(spark, lake):
    failure_day = COMPRESSOR_FAILURE["start"].date()
    expected = {lot.code for lot in World().lots
                if lot.room == COMPRESSOR_FAILURE["room"] and lot.produced_on <= failure_day <= lot.loading_date}
    exposed = {r.lot_code for r in gold(spark, lake, "fact_lot_cold_chain")
               .where("critical_exposure_min > 0").collect()}
    assert expected and exposed == expected


def test_failed_reefer_compromises_its_lots(spark, lake):
    failing = next(s for s in World().shipments if s.reefer_failure)
    transit = gold(spark, lake, "fact_container_transit")
    out = transit.where("transit_out_of_range_h > 0").collect()
    assert [r.shipment_number for r in out] == [failing.number]
    lots = gold(spark, lake, "fact_lot_cold_chain").where(f"shipment_number = '{failing.number}'").collect()
    assert lots and {l.cold_chain_status for l in lots} == {"COMPROMETIDO"}


def test_late_sensor_data_appears_in_the_next_publication(spark, lake):
    late_days = "room_code = 'GT01-ESC-01' AND reading_date BETWEEN '2026-10-10' AND '2026-10-12'"
    assert published(spark, lake, "fact_cold_room_daily", 1).where(late_days).count() == 0
    days = published(spark, lake, "fact_cold_room_daily", 2).where(late_days).collect()
    assert len(days) == 3 and all(d.coverage_pct > 99 for d in days)


def test_daily_intervals_reconcile_with_silver(spark, lake):
    readings = spark.read.format("delta").load(lake.paths.table("silver", "cold_room_readings"))
    expected = readings.select("room_code", "reading_ts_utc").distinct().count()
    daily = gold(spark, lake, "fact_cold_room_daily").agg(F.sum("intervals")).first()[0]
    assert daily == expected


def test_eta_updates_mean_late_arrivals(spark, lake):
    events = spark.read.format("delta").load(lake.paths.table("silver", "carrier_events"))
    updated = {r.container_number for r in events.where("event_type = 'ETA_UPDATED'")
               .select("container_number").distinct().collect()}
    delays = {r.container_number: r.arrival_delay_h for r in gold(spark, lake, "fact_container_transit").collect()}
    assert updated and all(delays[c] >= 24 for c in updated if c in delays)


def test_every_lot_has_one_status(spark, lake):
    lots = gold(spark, lake, "fact_lot_cold_chain")
    silver_lots = spark.read.format("delta").load(lake.paths.table("silver", "lots")).count()
    assert lots.count() == silver_lots == lots.select("lot_code").distinct().count()
    assert {r[0] for r in lots.select("cold_chain_status").distinct().collect()} <= {"OK", "EN_RIESGO", "COMPROMETIDO"}
