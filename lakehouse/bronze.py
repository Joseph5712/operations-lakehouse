"""Capa Bronze: ingesta idempotente de los archivos de la zona de aterrizaje.

Cada ejecución es un lote (`batch_id`). Para cada archivo nuevo:
  1. Se calcula su huella SHA-256.
  2. Si la huella ya se ingirió (aunque con otro nombre), se registra como
     SKIPPED_DUPLICATE y no se cargan filas.
  3. Si un CSV no trae las columnas del contrato, se registra como
     REJECTED_SCHEMA.
  4. Los demás se agregan a su tabla Delta con metadatos de linaje.

Bronze conserva el contenido original: los CSV se guardan como texto, columna
por columna, y cada evento JSON se guarda como la línea completa (`payload`).
Así un cambio de esquema en la fuente nunca rompe la ingesta.

Registro de ingesta: `ingestion_log` guarda una fila por archivo procesado y
se escribe al final del lote. Un lote solo existe si está en el registro. Si
una ejecución se interrumpe después de escribir datos y antes de registrarlos,
la siguiente elimina primero esas filas huérfanas.
"""
from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, LongType, StringType, StructField, StructType,
                               TimestampType)

from .config import Paths, table_exists
from .contracts import ERP_LOT_COLUMNS, ERP_PRODUCT_COLUMNS, ERP_SHIPMENT_COLUMNS, SENSOR_COLUMNS

INGESTED, SKIPPED_DUPLICATE, REJECTED_SCHEMA = "INGESTED", "SKIPPED_DUPLICATE", "REJECTED_SCHEMA"


@dataclass(frozen=True)
class BronzeTable:
    name: str
    source: str
    pattern: str
    columns: list[str] | None          # None: JSON Lines, se guarda la línea completa
    partition_by_arrival: bool = False


TABLES = (
    BronzeTable("erp_products", "erp", "products.csv", ERP_PRODUCT_COLUMNS),
    BronzeTable("erp_lots", "erp", "lots_*.csv", ERP_LOT_COLUMNS),
    BronzeTable("erp_shipments", "erp", "shipments_*.csv", ERP_SHIPMENT_COLUMNS),
    BronzeTable("sensor_readings", "sensors", "*.csv", SENSOR_COLUMNS, partition_by_arrival=True),
    BronzeTable("carrier_events", "carrier", "*.jsonl", None, partition_by_arrival=True),
)

LOG_TABLE = "ingestion_log"
LOG_SCHEMA = StructType([
    StructField("batch_id", LongType(), False),
    StructField("table_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("arrival_date", DateType(), False),
    StructField("file_sha256", StringType(), False),
    StructField("size_bytes", LongType(), False),
    StructField("rows", LongType(), True),
    StructField("status", StringType(), False),
    StructField("duplicate_of", StringType(), True),
    StructField("ingested_at", TimestampType(), False),
])


class SimulatedFailure(RuntimeError):
    """Interrupción provocada entre la escritura de datos y el registro del lote."""


@dataclass
class LandedFile:
    table: BronzeTable
    path: Path
    relative_path: str
    arrival_date: date
    sha256: str
    size_bytes: int


@dataclass
class IngestResult:
    batch_id: int | None
    files: Counter = field(default_factory=Counter)       # (tabla, estado) -> archivos
    rows: Counter = field(default_factory=Counter)        # tabla -> filas agregadas
    recovered_rows: int = 0


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _discover(paths: Paths) -> list[LandedFile]:
    landing = paths.landing_dir
    found = []
    for table in TABLES:
        for path in sorted((landing / table.source).glob(f"arrival_date=*/{table.pattern}")):
            arrival = date.fromisoformat(path.parent.name.split("=", 1)[1])
            found.append(LandedFile(table, path, str(path.relative_to(landing)), arrival,
                                    file_sha256(path), path.stat().st_size))
    return found


def _header_matches(f: LandedFile) -> bool:
    if f.table.columns is None:
        return True
    with f.path.open(encoding="utf-8-sig") as handle:
        header = handle.readline().strip().split(",")
    return header == f.table.columns


def _table_exists(path: str) -> bool:
    return table_exists(path)


def _read_log(spark: SparkSession, paths: Paths) -> DataFrame | None:
    path = paths.table("bronze", LOG_TABLE)
    return spark.read.format("delta").load(path) if _table_exists(path) else None


def _recover(spark: SparkSession, paths: Paths, committed: set[int]) -> int:
    """Elimina filas de lotes que no llegaron a registrarse."""
    removed = 0
    for table in TABLES:
        path = paths.table("bronze", table.name)
        if not _table_exists(path):
            continue
        condition = (~F.col("_batch_id").isin(list(committed))) if committed else F.lit(True)
        orphans = spark.read.format("delta").load(path).where(condition).count()
        if orphans:
            DeltaTable.forPath(spark, path).delete(condition)
            removed += orphans
    return removed


def _read_files(spark: SparkSession, paths: Paths, table: BronzeTable, files: list[LandedFile]) -> DataFrame:
    file_paths = [paths.landing_uri(f.relative_path) for f in files]
    if table.columns is None:
        df = spark.read.text(file_paths).withColumnRenamed("value", "payload")
    else:
        schema = StructType([StructField(c, StringType(), True) for c in table.columns])
        df = spark.read.option("header", True).schema(schema).csv(file_paths)

    # Linaje: se identifica cada archivo por su carpeta de llegada y su nombre,
    # lo que funciona igual con rutas locales y con rutas abfss:// de OneLake.
    lineage = spark.createDataFrame(
        [(f.arrival_date.isoformat(), f.path.name, f.relative_path, f.sha256) for f in files],
        "arrival_key string, file_name string, _source_file string, _file_sha256 string")
    keyed = (df
             .withColumn("arrival_key", F.regexp_extract(F.col("_metadata.file_path"), r"arrival_date=([^/]+)/", 1))
             .withColumn("file_name", F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)$", 1)))
    return (keyed.join(F.broadcast(lineage), ["arrival_key", "file_name"], "inner")
            .withColumn("_arrival_date", F.to_date("arrival_key"))
            .drop("arrival_key", "file_name"))


def ingest(spark: SparkSession, paths: Paths, fail_before_commit: bool = False) -> IngestResult:
    log = _read_log(spark, paths)
    if log is None:
        committed, known_paths, known_hashes = set(), set(), {}
    else:
        rows = log.select("batch_id", "file_path", "file_sha256", "status").collect()
        committed = {r.batch_id for r in rows}
        known_paths = {r.file_path for r in rows}
        known_hashes = {r.file_sha256: r.file_path for r in rows if r.status == INGESTED}

    result = IngestResult(batch_id=None)
    result.recovered_rows = _recover(spark, paths, committed)
    batch_id = max(committed, default=0) + 1

    to_load: dict[str, list[LandedFile]] = {}
    log_rows = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for f in _discover(paths):
        if f.relative_path in known_paths:
            continue
        if f.sha256 in known_hashes:
            status, duplicate_of = SKIPPED_DUPLICATE, known_hashes[f.sha256]
        elif not _header_matches(f):
            status, duplicate_of = REJECTED_SCHEMA, None
        else:
            status, duplicate_of = INGESTED, None
            known_hashes[f.sha256] = f.relative_path
            to_load.setdefault(f.table.name, []).append(f)
        log_rows.append([batch_id, f.table.name, f.relative_path, f.arrival_date, f.sha256,
                         f.size_bytes, None, status, duplicate_of, now])
        result.files[(f.table.name, status)] += 1

    if not log_rows:
        return result

    rows_per_file: dict[str, int] = {}
    for table in TABLES:
        files = to_load.get(table.name)
        if not files:
            continue
        df = (_read_files(spark, paths, table, files)
              .withColumn("_batch_id", F.lit(batch_id).cast("long"))
              .withColumn("_ingested_at", F.lit(now).cast("timestamp"))
              .cache())
        for r in df.groupBy("_source_file").count().collect():
            rows_per_file[r["_source_file"]] = r["count"]
        writer = df.write.format("delta").mode("append")
        if table.partition_by_arrival:
            writer = writer.partitionBy("_arrival_date")
        writer.save(paths.table("bronze", table.name))
        result.rows[table.name] += sum(rows_per_file.get(f.relative_path, 0) for f in files)
        df.unpersist()

    if fail_before_commit:
        raise SimulatedFailure("Interrupción simulada: datos escritos, lote sin registrar.")

    for row in log_rows:
        if row[7] == INGESTED:
            row[6] = rows_per_file.get(row[2], 0)
    (spark.createDataFrame(log_rows, LOG_SCHEMA)
     .write.format("delta").mode("append").save(paths.table("bronze", LOG_TABLE)))
    result.batch_id = batch_id
    return result
