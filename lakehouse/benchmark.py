"""Medición del pipeline completo con un volumen configurable.

El volumen de lecturas crece con el número de sensores por cuarto: con 2
sensores hay unas 176 mil lecturas en el periodo; con 20, unos 1.76 millones.
Las llegadas se procesan en dos tandas (hasta el 13 de octubre y el resto) para
medir también el costo de una carga incremental.
"""
from __future__ import annotations

import shutil
import statistics
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import gold, maintenance, quality, silver
from .bronze import ingest
from .config import Paths
from .generator import write_landing
from .generator.catalog import LAST_ARRIVAL

FIRST_ARRIVAL_CUT = date(2026, 10, 13)


@dataclass
class BenchmarkResult:
    sensors_per_room: int
    stages: list[tuple[str, float]] = field(default_factory=list)
    readings: int = 0
    query_before_s: float = 0.0
    query_after_s: float = 0.0
    files_before: int = 0
    files_after: int = 0


def _timed(result: BenchmarkResult, stage: str, action) -> None:
    start = time.perf_counter()
    action()
    result.stages.append((stage, time.perf_counter() - start))


def _point_query(spark: SparkSession, paths: Paths, repeats: int = 5) -> float:
    """Mediana de una consulta típica: un cuarto en un día."""
    readings = spark.read.format("delta").load(paths.table("silver", "cold_room_readings"))
    query = readings.where((F.col("reading_date") == "2026-10-07") & (F.col("room_code") == "CR01-LIM-02"))
    query.agg(F.max("temperature_c")).collect()                     # calentamiento
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        query.agg(F.max("temperature_c")).collect()
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def run(spark: SparkSession, root: Path, sensors_per_room: int) -> BenchmarkResult:
    shutil.rmtree(root, ignore_errors=True)
    paths = Paths(str(root))
    landing = Path(paths.files) / "landing"
    result = BenchmarkResult(sensors_per_room)

    _timed(result, "Generación (todas las llegadas)",
           lambda: write_landing(landing, until=LAST_ARRIVAL, sensors_per_room=sensors_per_room))
    # La primera tanda se simula moviendo fuera las llegadas posteriores al corte.
    later = root / "_later"
    for folder in landing.glob("*/arrival_date=*"):
        if date.fromisoformat(folder.name.split("=")[1]) > FIRST_ARRIVAL_CUT:
            target = later / folder.relative_to(landing)
            target.parent.mkdir(parents=True, exist_ok=True)
            folder.rename(target)

    _timed(result, "Bronze · carga inicial", lambda: ingest(spark, paths))
    _timed(result, "Silver · carga inicial", lambda: silver.build(spark, paths))
    _timed(result, "Gold · publicación 1", lambda: gold.publish(spark, paths))

    for folder in later.glob("*/arrival_date=*"):
        target = landing / folder.relative_to(later)
        target.parent.mkdir(parents=True, exist_ok=True)
        folder.rename(target)
    _timed(result, "Bronze · incremental", lambda: ingest(spark, paths))
    _timed(result, "Silver · incremental", lambda: silver.build(spark, paths))
    _timed(result, "Gold · publicación 2", lambda: gold.publish(spark, paths))
    _timed(result, "Controles de calidad", lambda: quality.run(spark, paths))
    _timed(result, "Silver · reproceso completo", lambda: silver.build(spark, paths, full_refresh=True))

    readings_path = paths.table("silver", "cold_room_readings")
    result.readings = spark.read.format("delta").load(readings_path).count()
    result.files_before = maintenance.file_count(spark, readings_path)
    result.query_before_s = _point_query(spark, paths)
    _timed(result, "OPTIMIZE de todas las tablas", lambda: maintenance.optimize(spark, paths))
    result.files_after = maintenance.file_count(spark, readings_path)
    result.query_after_s = _point_query(spark, paths)
    return result


def to_markdown(results: list[BenchmarkResult]) -> str:
    header = "| Etapa | " + " | ".join(f"{r.sensors_per_room} sensores/cuarto" for r in results) + " |"
    lines = [header, "|---|" + "---:|" * len(results)]
    lines.append("| Lecturas en Silver | " + " | ".join(f"{r.readings:,}" for r in results) + " |")
    for i, (stage, _) in enumerate(results[0].stages):
        lines.append(f"| {stage} | " + " | ".join(f"{r.stages[i][1]:.1f} s" for r in results) + " |")
    lines.append("| Archivos de `silver.cold_room_readings` antes / después de OPTIMIZE | "
                 + " | ".join(f"{r.files_before:,} / {r.files_after:,}" for r in results) + " |")
    lines.append("| Consulta de un cuarto en un día, antes / después de OPTIMIZE | "
                 + " | ".join(f"{r.query_before_s:.2f} s / {r.query_after_s:.2f} s" for r in results) + " |")
    return "\n".join(lines)
