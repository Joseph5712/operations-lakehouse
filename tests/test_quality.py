from lakehouse.quality import _unique


def statuses(checks):
    return {(c.name, c.target): c.status for c in checks}


def test_missing_sensor_days_are_reported_until_late_data_arrives(lake):
    before, after = lake.quality
    coverage = next(c for c in before if c.name == "cobertura_sensores")
    assert coverage.status == "WARN"
    assert "3 días sin lecturas" in coverage.observed and "GT01-ESC-01 2026-10-10" in coverage.observed
    assert statuses(after)[("cobertura_sensores", "gold.fact_cold_room_daily")] == "PASS"


def test_no_check_fails_on_a_healthy_pipeline(lake):
    for run in lake.quality:
        assert [c for c in run if c.status == "FAIL"] == []
    assert {c.status for c in lake.quality[1]} == {"PASS"}


def test_results_are_stored_per_run(spark, lake):
    results = spark.read.format("delta").load(lake.paths.table("gold", "quality_checks"))
    assert results.select("run_at").distinct().count() == 2
    assert results.count() == sum(len(run) for run in lake.quality)


def test_duplicate_keys_fail(spark):
    df = spark.createDataFrame([("L1", 1), ("L1", 2), ("L2", 3)], "lot_code string, qty int")
    check = _unique(spark, df, "clave_unica", "prueba", ["lot_code"])
    assert check.status == "FAIL" and check.observed == "1 claves repetidas"
