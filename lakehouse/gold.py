"""Capa Gold: modelo para el análisis de cadena de frío y tránsito.

Tablas:
  dim_cold_room              cuartos fríos con su rango de temperatura
  dim_product                productos
  fact_cold_room_daily       un cuarto por día: cobertura, temperaturas y minutos fuera de rango
  fact_cold_room_excursion   un episodio continuo fuera de rango de al menos 30 minutos
  fact_lot_cold_chain        un lote: exposición en cuarto frío y en tránsito, y su estado
  fact_container_transit     un embarque: zarpe, arribo, retraso y temperatura del contenedor

Publicación: Delta Lake confirma cada tabla por separado, así que una lectura
podría ver tablas de dos ejecuciones distintas. Para evitarlo, cada ejecución
agrega sus filas con un `_publication_id` nuevo y, al final, registra la
publicación en `_publications`. El consumo lee solo la última publicación
registrada (`current`). Se conserva la publicación anterior para poder volver
a ella; las más antiguas se eliminan.
"""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import Paths

PUBLICATIONS = "_publications"
REFERENCE_ROOMS = Path(__file__).parent / "reference" / "cold_rooms.csv"
REFERENCE_RULES = Path(__file__).parent / "reference" / "temp_class_rules.csv"

READING_INTERVAL_MIN = 5
EXCURSION_MIN_DURATION_MIN = 30        # episodios más cortos (descongelamiento, puerta) son transitorios
CRITICAL_DURATION_MIN = 120
REEFER_INTERVAL_H = 6
LOT_AT_RISK_ROOM_MIN = 60
LOT_CRITICAL_TRANSIT_H = 24

TABLES = ("dim_cold_room", "dim_product", "fact_cold_room_daily", "fact_cold_room_excursion",
          "fact_lot_cold_chain", "fact_container_transit")


class SimulatedFailure(RuntimeError):
    """Interrupción provocada después de escribir las tablas y antes de registrar la publicación."""


@dataclass
class GoldResult:
    publication_id: int | None
    skipped: bool = False
    rows: dict[str, int] = field(default_factory=dict)
    removed_orphan_rows: int = 0


def _exists(path: str) -> bool:
    return (Path(path) / "_delta_log").exists()


def _silver(spark: SparkSession, paths: Paths, table: str) -> DataFrame:
    return spark.read.format("delta").load(paths.table("silver", table)).drop(
        "_source_file", "_bronze_batch_id", "_silver_processed_at")


# --- Dimensiones -------------------------------------------------------------------------

def dim_product(spark: SparkSession, paths: Paths) -> DataFrame:
    return _silver(spark, paths, "products")


def _reference(spark: SparkSession, path: Path) -> DataFrame:
    with path.open(encoding="utf-8") as handle:
        return spark.createDataFrame(list(csv.DictReader(handle)))


def temp_class_ranges(spark: SparkSession, paths: Paths) -> DataFrame:
    """Rango de conservación por clase (del maestro) y umbrales de alarma (rango ± tolerancia)."""
    rules = _reference(spark, REFERENCE_RULES).select(
        "temp_class", F.col("alarm_tolerance_c").cast("double").alias("alarm_tolerance_c"))
    return (dim_product(spark, paths).groupBy("temp_class")
            .agg(F.max("min_temp_c").alias("min_temp_c"), F.min("max_temp_c").alias("max_temp_c"))
            .join(rules, "temp_class", "left")
            .withColumn("alarm_tolerance_c", F.coalesce("alarm_tolerance_c", F.lit(0.0)))
            .withColumn("alarm_low_c", F.col("min_temp_c") - F.col("alarm_tolerance_c"))
            .withColumn("alarm_high_c", F.col("max_temp_c") + F.col("alarm_tolerance_c")))


def dim_cold_room(spark: SparkSession, paths: Paths) -> DataFrame:
    rooms = _reference(spark, REFERENCE_ROOMS)
    return rooms.join(temp_class_ranges(spark, paths), "temp_class", "left").select(
        "room_code", "company_code", "site_name", "temp_class", "min_temp_c", "max_temp_c",
        "alarm_tolerance_c", "alarm_low_c", "alarm_high_c")


# --- Cuartos fríos ----------------------------------------------------------------------

def room_points(spark: SparkSession, paths: Paths, rooms: DataFrame) -> DataFrame:
    """Serie de cada cuarto cada 5 minutos, marcada por episodio fuera de rango."""
    points = (_silver(spark, paths, "cold_room_readings")
              .groupBy("room_code", "reading_ts_utc")
              .agg(F.avg("temperature_c").alias("temperature_c"), F.count("*").alias("sensors"))
              .join(F.broadcast(rooms.select("room_code", "alarm_low_c", "alarm_high_c")), "room_code"))
    by_time = Window.partitionBy("room_code").orderBy("reading_ts_utc")
    out = (F.col("temperature_c") < F.col("alarm_low_c")) | (F.col("temperature_c") > F.col("alarm_high_c"))
    previous_ts = F.lag("reading_ts_utc").over(by_time)
    gap = F.unix_timestamp("reading_ts_utc") - F.unix_timestamp(previous_ts) > READING_INTERVAL_MIN * 60
    flagged = points.withColumn("out_of_range", out).withColumn("_prev_out", F.lag(out).over(by_time))
    # Un episodio empieza cuando la serie sale de rango o cuando hay un hueco sin lecturas.
    starts = F.col("out_of_range") & (F.col("_prev_out").isNull() | ~F.col("_prev_out") | gap)
    return (flagged.withColumn("episode_id", F.sum(starts.cast("int")).over(by_time))
            .drop("_prev_out"))


def excursions(points: DataFrame, rooms: DataFrame) -> DataFrame:
    episodes = (points.where("out_of_range")
                .drop("alarm_low_c", "alarm_high_c")
                .groupBy("room_code", "episode_id")
                .agg(F.min("reading_ts_utc").alias("start_utc"),
                     F.max("reading_ts_utc").alias("_last_ts"),
                     F.count("*").alias("_slots"),
                     F.max("temperature_c").alias("_max_t"),
                     F.min("temperature_c").alias("_min_t"))
                .join(F.broadcast(rooms), "room_code"))
    high = F.col("_max_t") > F.col("max_temp_c")
    deviation = F.when(high, F.col("_max_t") - F.col("max_temp_c")).otherwise(F.col("min_temp_c") - F.col("_min_t"))
    duration = F.col("_slots") * READING_INTERVAL_MIN
    return (episodes
            .withColumn("end_utc", F.col("_last_ts") + F.expr(f"INTERVAL {READING_INTERVAL_MIN} MINUTES"))
            .withColumn("duration_min", duration)
            .withColumn("direction", F.when(high, "ALTA").otherwise("BAJA"))
            .withColumn("peak_temp_c", F.round(F.when(high, F.col("_max_t")).otherwise(F.col("_min_t")), 2))
            .withColumn("max_deviation_c", F.round(deviation, 2))
            .where(duration >= EXCURSION_MIN_DURATION_MIN)
            .withColumn("severity", F.when(duration >= CRITICAL_DURATION_MIN, "CRITICA").otherwise("MODERADA"))
            .withColumn("excursion_date", F.to_date("start_utc"))
            .select("room_code", "company_code", "temp_class", "episode_id", "excursion_date", "start_utc",
                    "end_utc", "duration_min", "direction", "peak_temp_c", "min_temp_c", "max_temp_c",
                    "max_deviation_c", "severity"))


def room_daily(points: DataFrame, room_excursions: DataFrame) -> DataFrame:
    in_excursion = room_excursions.select("room_code", "episode_id", F.lit(True).alias("in_excursion"))
    slots_per_day = 24 * 60 // READING_INTERVAL_MIN
    return (points.join(in_excursion, ["room_code", "episode_id"], "left")
            .groupBy("room_code", F.to_date("reading_ts_utc").alias("reading_date"))
            .agg(F.count("*").alias("intervals"),
                 F.round(F.avg("temperature_c"), 2).alias("avg_temp_c"),
                 F.round(F.min("temperature_c"), 2).alias("min_temp_c"),
                 F.round(F.max("temperature_c"), 2).alias("max_temp_c"),
                 (F.sum(F.col("out_of_range").cast("int")) * READING_INTERVAL_MIN).alias("minutes_out_of_range"),
                 # Las lecturas en rango conservan el número del último episodio: solo cuentan las de fuera de rango.
                 (F.sum((F.col("out_of_range") & F.coalesce("in_excursion", F.lit(False))).cast("int"))
                  * READING_INTERVAL_MIN).alias("excursion_minutes"))
            .withColumn("coverage_pct", F.round(F.col("intervals") * 100 / slots_per_day, 1)))


# --- Tránsito ---------------------------------------------------------------------------

def container_transit(spark: SparkSession, paths: Paths) -> DataFrame:
    shipments = _silver(spark, paths, "shipments")
    events = _silver(spark, paths, "carrier_events")
    lots = _silver(spark, paths, "lots")
    ranges = temp_class_ranges(spark, paths).select("temp_class", "alarm_low_c", "alarm_high_c")

    def first_time(event_type: str, alias: str) -> DataFrame:
        return (events.where(F.col("event_type") == event_type)
                .groupBy("container_number").agg(F.min("event_time").alias(alias)))

    latest_eta = (events.where("event_type = 'ETA_UPDATED'")
                  .withColumn("_r", F.row_number().over(
                      Window.partitionBy("container_number").orderBy(F.col("event_time").desc())))
                  .where("_r = 1").select("container_number", F.col("eta").alias("latest_eta")))
    reefer = (events.where("event_type = 'REEFER_READING'")
              .select("container_number", "reefer_supply_air_c"))
    lot_totals = (lots.where(F.col("shipment_number").isNotNull())
                  .groupBy("shipment_number").agg(F.count("*").alias("lots"), F.sum("qty_kg").alias("qty_kg")))

    base = (shipments
            .join(first_time("LOADED", "loaded_at"), "container_number", "left")
            .join(first_time("DEPARTED", "departed_at"), "container_number", "left")
            .join(first_time("ARRIVED", "arrived_at"), "container_number", "left")
            .join(latest_eta, "container_number", "left")
            .join(F.broadcast(ranges), "temp_class", "left")
            .join(lot_totals, "shipment_number", "left"))
    reefer_stats = (reefer.join(base.select("container_number", "alarm_low_c", "alarm_high_c"), "container_number")
                    .groupBy("container_number")
                    .agg(F.count("*").alias("reefer_readings"),
                         F.sum(((F.col("reefer_supply_air_c") < F.col("alarm_low_c"))
                                | (F.col("reefer_supply_air_c") > F.col("alarm_high_c"))).cast("int"))
                         .alias("_out_readings"),
                         F.max("reefer_supply_air_c").alias("max_supply_air_c")))
    expected_arrival = F.coalesce("arrived_at", "latest_eta", "planned_arrival")
    return (base.join(reefer_stats, "container_number", "left")
            .withColumn("status", F.when(F.col("arrived_at").isNotNull(), "ARRIBADO")
                        .when(F.col("departed_at").isNotNull(), "EN_TRANSITO").otherwise("EN_PUERTO"))
            .withColumn("arrival_delay_h", F.round(
                (F.unix_timestamp(expected_arrival) - F.unix_timestamp("planned_arrival")) / 3600, 1))
            .withColumn("transit_out_of_range_h", F.coalesce(F.col("_out_readings"), F.lit(0)) * REEFER_INTERVAL_H)
            .select("shipment_number", "company_code", "container_number", "carrier_code", "port_of_loading",
                    "port_of_discharge", "temp_class", "planned_departure", "planned_arrival", "loaded_at",
                    "departed_at", "arrived_at", "latest_eta", "status", "arrival_delay_h",
                    F.coalesce("reefer_readings", F.lit(0)).alias("reefer_readings"),
                    "transit_out_of_range_h", "max_supply_air_c",
                    F.coalesce("lots", F.lit(0)).alias("lots"), F.coalesce("qty_kg", F.lit(0)).alias("qty_kg")))


# --- Lotes ------------------------------------------------------------------------------

def lot_cold_chain(spark: SparkSession, paths: Paths, room_excursions: DataFrame,
                   transit: DataFrame, horizon) -> DataFrame:
    lots = _silver(spark, paths, "lots")
    products = dim_product(spark, paths).select("product_code", "product_name", "temp_class")
    # El lote está en su cuarto desde el inicio de su día de producción hasta que se carga
    # en el contenedor (evento LOADED o, en su defecto, 4 horas antes del zarpe planificado).
    stay = (lots.join(transit.select("shipment_number", "loaded_at", "planned_departure",
                                     "transit_out_of_range_h"), "shipment_number", "left")
            .withColumn("in_room_from", F.to_timestamp("production_date"))
            .withColumn("in_room_until", F.coalesce(
                "loaded_at", F.col("planned_departure") - F.expr("INTERVAL 4 HOURS"), F.lit(horizon))))
    exposure = (stay.alias("l")
                .join(room_excursions.alias("e"),
                      (F.col("l.cold_room_code") == F.col("e.room_code"))
                      & (F.col("e.start_utc") < F.col("l.in_room_until"))
                      & (F.col("e.end_utc") > F.col("l.in_room_from")))
                .withColumn("_overlap_min", (F.unix_timestamp(F.least("e.end_utc", "l.in_room_until"))
                                             - F.unix_timestamp(F.greatest("e.start_utc", "l.in_room_from"))) / 60)
                .groupBy("l.lot_code")
                .agg(F.sum("_overlap_min").cast("int").alias("room_exposure_min"),
                     F.sum(F.when(F.col("e.severity") == "CRITICA", F.col("_overlap_min")).otherwise(0))
                     .cast("int").alias("critical_exposure_min"),
                     F.count("*").alias("room_excursions")))
    room_min = F.coalesce("room_exposure_min", F.lit(0))
    critical_min = F.coalesce("critical_exposure_min", F.lit(0))
    transit_h = F.coalesce("transit_out_of_range_h", F.lit(0))
    return (stay.join(exposure, "lot_code", "left").join(F.broadcast(products), "product_code", "left")
            .withColumn("room_exposure_min", room_min)
            .withColumn("critical_exposure_min", critical_min)
            .withColumn("room_excursions", F.coalesce("room_excursions", F.lit(0)))
            .withColumn("transit_out_of_range_h", transit_h)
            .withColumn("cold_chain_status",
                        F.when((critical_min > 0) | (transit_h >= LOT_CRITICAL_TRANSIT_H), "COMPROMETIDO")
                        .when((room_min >= LOT_AT_RISK_ROOM_MIN) | (transit_h > 0), "EN_RIESGO").otherwise("OK"))
            .select("lot_code", "company_code", "product_code", "product_name", "temp_class", "production_date",
                    "qty_kg", "cold_room_code", "shipment_number", "in_room_from", "in_room_until",
                    "room_excursions", "room_exposure_min", "critical_exposure_min", "transit_out_of_range_h",
                    "cold_chain_status"))


# --- Publicación ------------------------------------------------------------------------

def _publications(spark: SparkSession, paths: Paths) -> DataFrame | None:
    path = paths.table("gold", PUBLICATIONS)
    return spark.read.format("delta").load(path) if _exists(path) else None


def current_publication(spark: SparkSession, paths: Paths) -> int | None:
    pubs = _publications(spark, paths)
    return None if pubs is None else pubs.agg(F.max("publication_id")).first()[0]


def current(spark: SparkSession, paths: Paths, table: str) -> DataFrame:
    """Contenido de la última publicación registrada de una tabla Gold."""
    publication = current_publication(spark, paths)
    df = spark.read.format("delta").load(paths.table("gold", table))
    return df.where(F.col("_publication_id") == publication).drop("_publication_id")


def _silver_signature(spark: SparkSession, paths: Paths) -> int:
    state = spark.read.format("delta").load(paths.table("silver", "_processing_state"))
    return state.agg(F.min("last_bronze_batch")).first()[0] or 0


def _remove(spark: SparkSession, paths: Paths, condition) -> int:
    removed = 0
    for table in TABLES:
        path = paths.table("gold", table)
        if _exists(path):
            count = spark.read.format("delta").load(path).where(condition).count()
            if count:
                DeltaTable.forPath(spark, path).delete(condition)
                removed += count
    return removed


def publish(spark: SparkSession, paths: Paths, fail_before_register: bool = False) -> GoldResult:
    pubs = _publications(spark, paths)
    published = [] if pubs is None else [r.publication_id for r in pubs.select("publication_id").collect()]
    signature = _silver_signature(spark, paths)
    if pubs is not None:
        last = pubs.orderBy(F.col("publication_id").desc()).first()
        if last.source_bronze_batch == signature:
            return GoldResult(publication_id=last.publication_id, skipped=True)

    result = GoldResult(publication_id=None)
    # Filas de una publicación que no llegó a registrarse.
    orphan = ~F.col("_publication_id").isin(published) if published else F.lit(True)
    result.removed_orphan_rows = _remove(spark, paths, orphan)
    publication_id = max(published, default=0) + 1

    rooms = dim_cold_room(spark, paths).cache()
    products = dim_product(spark, paths).cache()
    points = room_points(spark, paths, rooms).cache()
    room_excursions = excursions(points, rooms).cache()
    transit = container_transit(spark, paths).cache()
    horizon = points.agg(F.max("reading_ts_utc")).first()[0]
    outputs = {
        "dim_cold_room": rooms,
        "dim_product": products,
        "fact_cold_room_daily": room_daily(points, room_excursions),
        "fact_cold_room_excursion": room_excursions.drop("episode_id"),
        "fact_lot_cold_chain": lot_cold_chain(spark, paths, room_excursions, transit, horizon),
        "fact_container_transit": transit,
    }
    for table, df in outputs.items():
        df = df.withColumn("_publication_id", F.lit(publication_id).cast("long"))
        df.write.format("delta").mode("append").save(paths.table("gold", table))
        result.rows[table] = df.count()

    if fail_before_register:
        raise SimulatedFailure("Interrupción simulada: tablas escritas, publicación sin registrar.")

    marker = spark.createDataFrame(
        [(publication_id, datetime.now(timezone.utc).replace(tzinfo=None), signature, json.dumps(result.rows))],
        "publication_id long, published_at timestamp, source_bronze_batch long, row_counts string")
    marker.write.format("delta").mode("append").save(paths.table("gold", PUBLICATIONS))
    result.publication_id = publication_id

    # Se conservan la publicación vigente y la anterior.
    _remove(spark, paths, F.col("_publication_id") < publication_id - 1)
    for df in (rooms, products, points, room_excursions, transit):
        df.unpersist()
    return result


def register_views(spark: SparkSession, paths: Paths) -> list[str]:
    """Vistas temporales `gold_<tabla>` con la publicación vigente, para consultas con Spark SQL."""
    names = []
    for table in TABLES:
        current(spark, paths, table).createOrReplaceTempView(f"gold_{table}")
        names.append(f"gold_{table}")
    return names
