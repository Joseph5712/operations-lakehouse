"""Capa Silver: datos tipados, validados y con un registro por clave de negocio.

Procesamiento incremental: cada tabla Silver recuerda el último lote de Bronze
que procesó (`_processing_state`) y en cada ejecución lee solo los lotes
nuevos y ya confirmados en el registro de ingesta.

Por cada tabla:
  1. Tipado: texto a fechas, números e instantes UTC.
  2. Validación: la primera regla incumplida se guarda como motivo y el
     registro va a `quarantine` con su contenido original.
  3. Deduplicación por clave de negocio dentro de los lotes nuevos.
  4. MERGE en la tabla Silver:
       lecturas y eventos  solo se insertan claves nuevas (son inmutables)
       lotes y embarques   se conserva la versión con mayor `updated_at`
       productos           se actualiza el maestro

El procesamiento es repetible: si una ejecución se interrumpe, la siguiente
vuelve a procesar los mismos lotes de Bronze sin duplicar datos ni
registros en cuarentena.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, StringType, StructField,
                               StructType)

from . import iso6346
from .config import Paths

STATE_TABLE = "_processing_state"
QUARANTINE_TABLE = "quarantine"
EVENT_TYPES = ["GATE_IN", "LOADED", "DEPARTED", "REEFER_READING", "ETA_UPDATED", "ARRIVED",
               "DISCHARGED", "GATE_OUT"]
COMPANIES = ["CR01", "GT01"]
TEMP_CLASSES = ["FRESH", "BANANA", "FROZEN"]
SENSOR_ERROR_CODE = -99.9
SENSOR_RANGE_C = (-40.0, 50.0)

# Esquema de la naviera: superconjunto de las versiones 1 y 2.
CARRIER_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("container", StructType([StructField("number", StringType()),
                                         StructField("iso_type", StringType())])),
    StructField("location", StructType([StructField("locode", StringType()),
                                        StructField("name", StringType())])),
    StructField("vessel", StructType([StructField("name", StringType()),
                                      StructField("voyage", StringType())])),
    StructField("reefer", StructType([StructField("setpoint_c", DoubleType()),
                                      StructField("supply_air_c", DoubleType()),
                                      StructField("return_air_c", DoubleType()),
                                      StructField("humidity_pct", DoubleType())])),
    StructField("eta", StringType()),
])

container_is_valid = F.udf(iso6346.is_valid, BooleanType())


@dataclass
class TableResult:
    bronze_rows: int = 0
    duplicates: int = 0
    quarantined: int = 0
    inserted: int = 0
    updated: int = 0


@dataclass
class SilverResult:
    tables: dict[str, TableResult] = field(default_factory=dict)
    bronze_batches: tuple[int, int] | None = None       # (desde, hasta) inclusive


def _exists(path: str) -> bool:
    return (Path(path) / "_delta_log").exists()


def _first_failure(*rules: tuple[Column, str]) -> Column:
    """Motivo de la primera regla incumplida, o NULL si el registro es válido."""
    reason = F.lit(None).cast("string")
    for failed, message in reversed(rules):
        reason = F.when(failed, F.lit(message)).otherwise(reason)
    return reason


def _latest(df: DataFrame, keys: list[str], order: list[Column]) -> tuple[DataFrame, int]:
    """Una fila por clave, según el orden indicado. Devuelve también cuántas se descartaron."""
    ranked = df.withColumn("_rank", F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))
    kept = ranked.where("_rank = 1").drop("_rank")
    return kept, df.count() - kept.count()


# --- Transformaciones por tabla ---------------------------------------------------------

def _products(df: DataFrame) -> DataFrame:
    return (df.select(
        F.col("product_code"), F.col("product_name"), F.col("temp_class"),
        F.col("min_temp_c").cast("double").alias("min_temp_c"),
        F.col("max_temp_c").cast("double").alias("max_temp_c"),
        F.col("shelf_life_days").cast("int").alias("shelf_life_days"),
        "_source_file", "_batch_id", "_raw")
        .withColumn("_reason", _first_failure(
            (F.col("product_code").isNull(), "product_code vacío"),
            (~F.col("temp_class").isin(TEMP_CLASSES), "clase de temperatura desconocida"),
            (F.col("min_temp_c").isNull() | F.col("max_temp_c").isNull(), "rango de temperatura no numérico"),
            (F.col("min_temp_c") >= F.col("max_temp_c"), "rango de temperatura invertido"))))


def _lots(df: DataFrame, products: DataFrame) -> DataFrame:
    known = products.select(F.col("product_code").alias("_known_product"))
    typed = (df.select(
        F.col("lot_code"), F.col("company_code"), F.col("product_code"),
        F.to_date("production_date").alias("production_date"),
        F.col("qty_kg").cast("int").alias("qty_kg"),
        F.col("cold_room_code"),
        F.expr("nullif(shipment_number, '')").alias("shipment_number"),
        F.to_timestamp("updated_at").alias("updated_at"),
        "_source_file", "_batch_id", "_raw")
        .join(F.broadcast(known), F.col("product_code") == F.col("_known_product"), "left"))
    return (typed.withColumn("_reason", _first_failure(
        (F.col("lot_code").isNull(), "lot_code vacío"),
        (~F.col("company_code").isin(COMPANIES), "sociedad desconocida"),
        (F.col("_known_product").isNull(), "producto inexistente en el maestro"),
        (F.col("production_date").isNull(), "production_date inválida"),
        (F.col("qty_kg").isNull(), "qty_kg no numérico"),
        (F.col("qty_kg") <= 0, "peso no positivo"),
        (F.col("updated_at").isNull(), "updated_at inválido")))
        .drop("_known_product"))


def _shipments(df: DataFrame) -> DataFrame:
    typed = df.select(
        F.col("shipment_number"), F.col("company_code"), F.col("container_number"),
        F.col("carrier_code"), F.col("port_of_loading"), F.col("port_of_discharge"), F.col("temp_class"),
        F.to_timestamp("planned_departure").alias("planned_departure"),
        F.to_timestamp("planned_arrival").alias("planned_arrival"),
        F.to_timestamp("updated_at").alias("updated_at"),
        "_source_file", "_batch_id", "_raw")
    return typed.withColumn("_reason", _first_failure(
        (F.col("shipment_number").isNull(), "shipment_number vacío"),
        (~container_is_valid("container_number"), "contenedor inválido según ISO 6346"),
        (~F.col("temp_class").isin(TEMP_CLASSES), "clase de temperatura desconocida"),
        (F.col("planned_departure").isNull() | F.col("planned_arrival").isNull(), "fechas planificadas inválidas"),
        (F.col("planned_arrival") <= F.col("planned_departure"), "ETA anterior al zarpe"),
        (F.col("updated_at").isNull(), "updated_at inválido")))


def _readings(df: DataFrame) -> DataFrame:
    temperature = F.col("temperature_c").cast("double")
    humidity = F.col("humidity_pct").cast("double")
    # La conversión respeta el desfase del texto ("Z" o "-06:00") y deja el instante en UTC.
    reading_ts = F.to_timestamp("reading_ts")
    typed = df.select(
        "room_code", "sensor_id",
        reading_ts.alias("reading_ts_utc"),
        F.to_date(reading_ts).alias("reading_date"),
        temperature.alias("temperature_c"),
        humidity.alias("humidity_pct"),
        F.col("temperature_c").alias("_raw_temperature"),
        "_source_file", "_batch_id", "_raw")
    low, high = SENSOR_RANGE_C
    return (typed.withColumn("_reason", _first_failure(
        (F.col("room_code").isNull() | F.col("sensor_id").isNull(), "cuarto o sensor vacío"),
        (F.col("reading_ts_utc").isNull(), "reading_ts inválido"),
        (F.col("_raw_temperature").isNull(), "temperatura vacía"),
        (F.col("temperature_c").isNull(), "temperatura no numérica"),
        (F.col("temperature_c") == SENSOR_ERROR_CODE, "código de error del sensor (-99.9)"),
        ((F.col("temperature_c") < low) | (F.col("temperature_c") > high), "temperatura fuera del rango del sensor"),
        (F.col("humidity_pct").isNull() | ~F.col("humidity_pct").between(0, 100), "humedad inválida")))
        .drop("_raw_temperature"))


def _events(df: DataFrame) -> DataFrame:
    e = F.from_json("payload", CARRIER_SCHEMA).alias("e")
    parsed = df.select(e, "_source_file", "_batch_id", "_raw")
    typed = parsed.select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.event_type").alias("event_type"),
        F.to_timestamp("e.event_time").alias("event_time"),
        F.col("e.container.number").alias("container_number"),
        F.col("e.location.locode").alias("locode"),
        F.col("e.location.name").alias("location_name"),
        F.col("e.vessel.name").alias("vessel_name"),
        F.col("e.vessel.voyage").alias("voyage"),
        F.col("e.reefer.setpoint_c").alias("reefer_setpoint_c"),
        F.col("e.reefer.supply_air_c").alias("reefer_supply_air_c"),
        F.col("e.reefer.return_air_c").alias("reefer_return_air_c"),
        F.col("e.reefer.humidity_pct").alias("reefer_humidity_pct"),       # solo en la versión 2
        F.to_timestamp("e.eta").alias("eta"),
        F.coalesce(F.col("e.schema_version"), F.lit(1)).alias("schema_version"),
        F.col("e").isNull().alias("_unparsed"),
        "_source_file", "_batch_id", "_raw")
    return (typed.withColumn("_reason", _first_failure(
        (F.col("_unparsed") | F.col("event_id").isNull(), "JSON inválido o sin event_id"),
        (~F.col("event_type").isin(EVENT_TYPES), "tipo de evento desconocido"),
        (F.col("event_time").isNull(), "event_time inválido"),
        (F.col("container_number").isNull(), "evento sin contenedor")))
        .drop("_unparsed"))


@dataclass(frozen=True)
class SilverTable:
    name: str
    bronze: str
    keys: list[str]
    mode: str                       # "insert" (inmutable) o "latest" (versión más reciente)
    partition_by: str | None = None


TABLES = (
    SilverTable("products", "erp_products", ["product_code"], "latest"),
    SilverTable("lots", "erp_lots", ["lot_code"], "latest"),
    SilverTable("shipments", "erp_shipments", ["shipment_number"], "latest"),
    SilverTable("cold_room_readings", "sensor_readings", ["room_code", "sensor_id", "reading_ts_utc"], "insert",
                partition_by="reading_date"),
    SilverTable("carrier_events", "carrier_events", ["event_id"], "insert"),
)


# --- Orquestación ---------------------------------------------------------------------------

def _state(spark: SparkSession, paths: Paths) -> dict[str, int]:
    path = paths.table("silver", STATE_TABLE)
    if not _exists(path):
        return {}
    return {r.table_name: r.last_bronze_batch for r in spark.read.format("delta").load(path).collect()}


def _save_state(spark: SparkSession, paths: Paths, table: str, batch: int) -> None:
    path = paths.table("silver", STATE_TABLE)
    row = spark.createDataFrame([(table, batch, datetime.now(timezone.utc).replace(tzinfo=None))],
                                "table_name string, last_bronze_batch long, processed_at timestamp")
    if not _exists(path):
        row.write.format("delta").save(path)
        return
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(row.alias("s"), "t.table_name = s.table_name")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def _committed_bronze_batch(spark: SparkSession, paths: Paths) -> int:
    path = paths.table("bronze", "ingestion_log")
    if not _exists(path):
        return 0
    return spark.read.format("delta").load(path).agg(F.max("batch_id")).first()[0] or 0


def _quarantine(spark: SparkSession, paths: Paths, table: SilverTable, rejected: DataFrame,
                first: int, last: int) -> int:
    path = paths.table("silver", QUARANTINE_TABLE)
    rows = rejected.select(
        F.lit(table.name).alias("silver_table"),
        F.col("_reason").alias("reason"),
        F.col("_raw").alias("record"),
        F.col("_source_file"),
        F.col("_batch_id").alias("_bronze_batch_id"),
        F.current_timestamp().alias("quarantined_at"))
    # Repetible: se reemplazan los registros de este mismo rango de lotes.
    if _exists(path):
        DeltaTable.forPath(spark, path).delete(
            (F.col("silver_table") == table.name) & F.col("_bronze_batch_id").between(first, last))
    count = rows.count()
    if count:
        rows.write.format("delta").mode("append").save(path)
    return count


def _merge(spark: SparkSession, path: str, table: SilverTable, df: DataFrame) -> tuple[int, int]:
    if not _exists(path):
        writer = df.write.format("delta")
        if table.partition_by:
            writer = writer.partitionBy(table.partition_by)
        writer.save(path)
        return df.count(), 0
    condition = " AND ".join(f"t.{k} = s.{k}" for k in table.keys)
    if table.partition_by:
        condition = f"t.{table.partition_by} = s.{table.partition_by} AND {condition}"
    target = DeltaTable.forPath(spark, path)
    version_before = target.history(1).first().version
    merge = target.alias("t").merge(df.alias("s"), condition)
    if table.mode == "latest":
        merge = merge.whenMatchedUpdateAll(condition="s.updated_at > t.updated_at" if "updated_at" in df.columns else None)
    merge.whenNotMatchedInsertAll().execute()
    last = DeltaTable.forPath(spark, path).history(1).first()
    if last.version == version_before:          # un MERGE sin cambios no crea una versión nueva
        return 0, 0
    metrics = last.operationMetrics
    return int(metrics.get("numTargetRowsInserted", 0)), int(metrics.get("numTargetRowsUpdated", 0))


def build(spark: SparkSession, paths: Paths) -> SilverResult:
    state = _state(spark, paths)
    last = _committed_bronze_batch(spark, paths)
    result = SilverResult()
    firsts = []

    for table in TABLES:
        first = state.get(table.name, 0) + 1
        if first > last:
            result.tables[table.name] = TableResult()
            continue
        firsts.append(first)
        bronze_path = paths.table("bronze", table.bronze)
        if not _exists(bronze_path):
            _save_state(spark, paths, table.name, last)
            result.tables[table.name] = TableResult()
            continue
        bronze = (spark.read.format("delta").load(bronze_path)
                  .where(F.col("_batch_id").between(first, last)))
        original_columns = [c for c in bronze.columns if not c.startswith("_")]
        # El registro tal como llegó, para guardarlo en cuarentena si no es válido.
        raw = F.col("payload") if original_columns == ["payload"] else F.to_json(F.struct(*original_columns))
        bronze = bronze.withColumn("_raw", raw)

        if table.name == "products":
            typed = _products(bronze)
        elif table.name == "lots":
            products = spark.read.format("delta").load(paths.table("silver", "products"))
            typed = _lots(bronze, products)
        elif table.name == "shipments":
            typed = _shipments(bronze)
        elif table.name == "cold_room_readings":
            typed = _readings(bronze)
        else:
            typed = _events(bronze)

        typed = typed.cache()
        stats = TableResult(bronze_rows=bronze.count())
        stats.quarantined = _quarantine(spark, paths, table, typed.where(F.col("_reason").isNotNull()),
                                        first, last)

        valid = typed.where(F.col("_reason").isNull()).drop("_reason", "_raw")
        if table.mode == "latest":
            # Gana la versión más reciente; sin updated_at, la del lote más reciente.
            order = ([F.col("updated_at").desc()] if "updated_at" in valid.columns else []) + \
                    [F.col("_batch_id").desc(), F.col("_source_file").desc()]
        else:
            # Registros inmutables: se conserva la primera vez que llegó.
            order = [F.col("_batch_id"), F.col("_source_file")]
        deduped, stats.duplicates = _latest(valid, table.keys, order)
        deduped = (deduped.withColumnRenamed("_batch_id", "_bronze_batch_id")
                   .withColumn("_silver_processed_at", F.current_timestamp()))

        stats.inserted, stats.updated = _merge(spark, paths.table("silver", table.name), table, deduped)
        _save_state(spark, paths, table.name, last)
        typed.unpersist()
        result.tables[table.name] = stats

    if firsts:
        result.bronze_batches = (min(firsts), last)
    return result

