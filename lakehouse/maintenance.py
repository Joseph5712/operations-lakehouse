"""Mantenimiento de las tablas Delta.

Las cargas incrementales y los MERGE dejan muchos archivos pequeños: cada lote
agrega al menos un archivo por partición. OPTIMIZE los compacta en archivos más
grandes y, con Z-ORDER, ordena las filas para que una consulta filtrada lea
menos archivos. VACUUM borra del disco los archivos que ya no pertenecen a
ninguna versión dentro del periodo de retención (7 días por defecto), por lo
que el historial reciente sigue disponible.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from .config import LAYERS, Paths

# Columnas por las que se filtra con más frecuencia.
ZORDER = {("silver", "cold_room_readings"): ["room_code"]}


@dataclass
class TableMaintenance:
    table: str
    files_before: int
    files_after: int
    seconds: float


def _tables(paths: Paths) -> list[tuple[str, str]]:
    found = []
    for layer in LAYERS:
        folder = Path(paths.root) / "Tables" / layer
        if folder.exists():
            found += [(layer, p.name) for p in sorted(folder.iterdir()) if (p / "_delta_log").exists()]
    return found


def file_count(spark: SparkSession, path: str) -> int:
    return DeltaTable.forPath(spark, path).detail().first()["numFiles"]


def optimize(spark: SparkSession, paths: Paths, vacuum: bool = False) -> list[TableMaintenance]:
    results = []
    for layer, name in _tables(paths):
        path = paths.table(layer, name)
        table = DeltaTable.forPath(spark, path)
        before = file_count(spark, path)
        start = time.perf_counter()
        builder = table.optimize()
        columns = ZORDER.get((layer, name))
        builder.executeZOrderBy(*columns) if columns else builder.executeCompaction()
        if vacuum:
            table.vacuum()
        results.append(TableMaintenance(f"{layer}.{name}", before, file_count(spark, path),
                                        time.perf_counter() - start))
    return results
