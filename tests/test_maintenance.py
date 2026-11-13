from delta.tables import DeltaTable

from lakehouse.maintenance import file_count, optimize


def test_optimize_keeps_data_and_history(spark, lake):
    paths = lake.paths
    readings = paths.table("silver", "cold_room_readings")
    rows_before = spark.read.format("delta").load(readings).count()
    version_before = DeltaTable.forPath(spark, readings).history(1).first().version
    bronze = paths.table("bronze", "sensor_readings")
    bronze_files = file_count(spark, bronze)

    results = {r.table: r for r in optimize(spark, paths)}

    assert all(r.files_after <= r.files_before for r in results.values())
    assert file_count(spark, bronze) < bronze_files
    assert spark.read.format("delta").load(readings).count() == rows_before
    old = spark.read.format("delta").option("versionAsOf", version_before).load(readings).count()
    assert old == rows_before
