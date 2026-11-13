"""Controles de calidad del lakehouse.

Se ejecutan al final del pipeline y guardan su resultado en `gold._quality_checks`.
Cada control produce PASS, WARN o FAIL:

  FAIL  el dato publicado no es confiable; el pipeline termina con error
  WARN  el dato es utilizable, pero hay algo que alguien debe revisar
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import Paths
from .gold import current, current_publication

RESULTS_TABLE = "_quality_checks"
QUARANTINE_WARN_RATE = 0.01         # más de 1 % de registros rechazados
FRESHNESS_WARN_DAYS = 2             # una fuente con más de 2 días de atraso respecto de las demás

SILVER_KEYS = {
    "products": ["product_code"],
    "lots": ["lot_code"],
    "shipments": ["shipment_number"],
    "cold_room_readings": ["room_code", "sensor_id", "reading_ts_utc"],
    "carrier_events": ["event_id"],
}
GOLD_KEYS = {
    "fact_cold_room_daily": ["room_code", "reading_date"],
    "fact_lot_cold_chain": ["lot_code"],
    "fact_container_transit": ["shipment_number"],
}
BRONZE_SOURCES = {"erp_lots": "erp", "sensor_readings": "sensors", "carrier_events": "carrier"}
SILVER_TO_BRONZE = {"products": "erp_products", "lots": "erp_lots", "shipments": "erp_shipments",
                    "cold_room_readings": "sensor_readings", "carrier_events": "carrier_events"}


@dataclass
class Check:
    name: str
    target: str
    status: str            # PASS, WARN o FAIL
    observed: str
    expectation: str


def _exists(path: str) -> bool:
    return (Path(path) / "_delta_log").exists()


def _delta(spark: SparkSession, path: str):
    return spark.read.format("delta").load(path)


def _unique(spark: SparkSession, df, name: str, target: str, keys: list[str]) -> Check:
    duplicated = df.groupBy(*keys).count().where("count > 1").count()
    return Check(name, target, "PASS" if duplicated == 0 else "FAIL",
                 f"{duplicated} claves repetidas", "0 claves repetidas")


def bronze_reconciliation(spark: SparkSession, paths: Paths) -> list[Check]:
    """Las filas registradas en ingestion_log coinciden con las filas de cada tabla Bronze."""
    log = _delta(spark, paths.table("bronze", "ingestion_log"))
    expected = {r.table_name: r.rows for r in log.where("status = 'INGESTED'")
                .groupBy("table_name").agg(F.sum("rows").alias("rows")).collect()}
    checks = []
    for table, rows in sorted(expected.items()):
        actual = _delta(spark, paths.table("bronze", table)).count()
        checks.append(Check("conciliacion_bronze", f"bronze.{table}", "PASS" if actual == rows else "FAIL",
                            f"{actual:,} filas en la tabla", f"{rows:,} filas registradas"))
    return checks


def silver_uniqueness(spark: SparkSession, paths: Paths) -> list[Check]:
    return [_unique(spark, _delta(spark, paths.table("silver", t)), "clave_unica", f"silver.{t}", keys)
            for t, keys in SILVER_KEYS.items() if _exists(paths.table("silver", t))]


def quarantine_rate(spark: SparkSession, paths: Paths) -> list[Check]:
    quarantine_path = paths.table("silver", "quarantine")
    rejected = {}
    if _exists(quarantine_path):
        rejected = {r.silver_table: r["count"] for r in _delta(spark, quarantine_path).groupBy("silver_table").count().collect()}
    checks = []
    for table, bronze_table in SILVER_TO_BRONZE.items():
        path = paths.table("bronze", bronze_table)
        if not _exists(path):
            continue
        total = _delta(spark, path).count()
        rate = rejected.get(table, 0) / total if total else 0.0
        checks.append(Check("tasa_cuarentena", f"silver.{table}", "PASS" if rate <= QUARANTINE_WARN_RATE else "WARN",
                            f"{rejected.get(table, 0):,} de {total:,} ({rate:.2%})", f"≤ {QUARANTINE_WARN_RATE:.0%}"))
    return checks


def source_freshness(spark: SparkSession, paths: Paths) -> list[Check]:
    """Una fuente que deja de enviar archivos mientras las demás siguen llegando."""
    log = _delta(spark, paths.table("bronze", "ingestion_log"))
    latest = {r.table_name: r.latest for r in log.groupBy("table_name").agg(F.max("arrival_date").alias("latest")).collect()}
    newest = max(latest.values())
    checks = []
    for table, source in BRONZE_SOURCES.items():
        if table not in latest:
            continue
        lag = (newest - latest[table]).days
        checks.append(Check("frescura_fuente", source, "PASS" if lag <= FRESHNESS_WARN_DAYS else "WARN",
                            f"última llegada {latest[table]} ({lag} días de atraso)",
                            f"≤ {FRESHNESS_WARN_DAYS} días respecto de la fuente más reciente"))
    return checks


def sensor_coverage(spark: SparkSession, paths: Paths) -> list[Check]:
    """Días sin lecturas dentro del periodo de cada cuarto frío (por ejemplo, una pasarela sin conexión)."""
    daily = current(spark, paths, "fact_cold_room_daily")
    bounds = daily.agg(F.min("reading_date").alias("first"), F.max("reading_date").alias("last")).first()
    expected = (spark.sql(f"SELECT explode(sequence(DATE'{bounds.first}', DATE'{bounds.last}')) AS reading_date")
                .crossJoin(current(spark, paths, "dim_cold_room").select("room_code")))
    missing = expected.join(daily, ["room_code", "reading_date"], "left_anti").orderBy("room_code", "reading_date").collect()
    detail = ", ".join(f"{r.room_code} {r.reading_date}" for r in missing[:5]) + (" …" if len(missing) > 5 else "")
    return [Check("cobertura_sensores", "gold.fact_cold_room_daily", "PASS" if not missing else "WARN",
                  f"{len(missing)} días sin lecturas" + (f": {detail}" if missing else ""), "0 días sin lecturas")]


def gold_checks(spark: SparkSession, paths: Paths) -> list[Check]:
    checks = [_unique(spark, current(spark, paths, t), "clave_unica", f"gold.{t}", keys) for t, keys in GOLD_KEYS.items()]
    signature = (_delta(spark, paths.table("silver", "_processing_state"))
                 .agg(F.min("last_bronze_batch")).first()[0])
    published = (_delta(spark, paths.table("gold", "_publications"))
                 .orderBy(F.col("publication_id").desc()).first().source_bronze_batch)
    checks.append(Check("gold_al_dia", "gold._publications", "PASS" if published == signature else "FAIL",
                        f"publicación con lote de Bronze {published}", f"lote de Bronze {signature} procesado en Silver"))
    lots = current(spark, paths, "fact_lot_cold_chain")
    rooms = current(spark, paths, "dim_cold_room").select(F.col("room_code").alias("cold_room_code"))
    orphan = lots.join(rooms, "cold_room_code", "left_anti").count()
    checks.append(Check("integridad_referencial", "gold.fact_lot_cold_chain", "PASS" if orphan == 0 else "FAIL",
                        f"{orphan} lotes con cuarto desconocido", "0 lotes con cuarto desconocido"))
    return checks


def run(spark: SparkSession, paths: Paths) -> list[Check]:
    if current_publication(spark, paths) is None:
        raise RuntimeError("No hay una publicación Gold: ejecuta primero el pipeline.")
    checks = (bronze_reconciliation(spark, paths) + silver_uniqueness(spark, paths) + quarantine_rate(spark, paths)
              + source_freshness(spark, paths) + gold_checks(spark, paths) + sensor_coverage(spark, paths))
    run_at = datetime.now(timezone.utc).replace(tzinfo=None)
    publication = current_publication(spark, paths)
    (spark.createDataFrame([(run_at, publication, c.name, c.target, c.status, c.observed, c.expectation) for c in checks],
                           "run_at timestamp, publication_id long, check_name string, target string, status string, "
                           "observed string, expectation string")
     .write.format("delta").mode("append").save(paths.table("gold", RESULTS_TABLE)))
    return checks

