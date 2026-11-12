from lakehouse.config import paths


def test_paths_follow_fabric_layout(lakehouse_root):
    p = paths()
    assert p.landing("sensors") == f"{lakehouse_root}/Files/landing/sensors"
    assert p.table("silver", "cold_room_reading") == f"{lakehouse_root}/Tables/silver/cold_room_reading"


def test_delta_time_travel(spark, lakehouse_root):
    target = paths().table("bronze", "check")
    spark.createDataFrame([(1, 10)], "id int, value int").write.format("delta").save(target)
    spark.createDataFrame([(1, 25)], "id int, value int").write.format("delta").mode("overwrite").save(target)

    assert spark.read.format("delta").load(target).first()["value"] == 25
    assert spark.read.format("delta").option("versionAsOf", 0).load(target).first()["value"] == 10
