"""Ejecución en Microsoft Fabric.

Los notebooks de `fabric/notebooks` llaman a `run_stage`: la lógica de cada capa
vive en este paquete y no se duplica en los notebooks. En Fabric la sesión de
Spark ya existe (`spark`) y el Lakehouse predeterminado del notebook está
montado en /lakehouse/default.

Cada etapa devuelve un resumen serializable que el notebook publica con
`notebookutils.notebook.exit` y el pipeline puede leer. Si un control de
calidad falla, `run_stage` lanza una excepción y la actividad del pipeline
queda en error.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import date

from pyspark.sql import SparkSession

from . import gold, quality, silver
from .bronze import INGESTED, REJECTED_SCHEMA, SKIPPED_DUPLICATE, ingest
from .config import Paths
from .generator import write_landing
from .generator.catalog import LAST_ARRIVAL

STAGES = ("generate", "bronze", "silver", "gold", "quality")
DEFAULT_MOUNT = "/lakehouse/default"


class QualityCheckFailed(RuntimeError):
    """Uno o más controles de calidad terminaron en FAIL."""


def onelake_root(workspace: str, lakehouse: str) -> str:
    """URI de OneLake de un Lakehouse a partir de los nombres del área de trabajo y del Lakehouse."""
    return f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse"


def fabric_paths(workspace: str, lakehouse: str, mount: str = DEFAULT_MOUNT) -> Paths:
    return Paths(onelake_root(workspace, lakehouse), mount)


def run_stage(spark: SparkSession, paths: Paths, stage: str, until: date | None = None,
              full_refresh: bool = False) -> dict:
    if stage not in STAGES:
        raise ValueError(f"Etapa desconocida: {stage}. Usa una de {STAGES}.")

    if stage == "generate":
        summary = write_landing(paths.landing_dir, until=min(until or LAST_ARRIVAL, LAST_ARRIVAL))
        return {"stage": stage, "files_written": dict(summary.written), "rows_written": dict(summary.rows)}

    if stage == "bronze":
        result = ingest(spark, paths)
        return {"stage": stage, "batch_id": result.batch_id, "recovered_rows": result.recovered_rows,
                "files": {status: sum(n for (_t, s), n in result.files.items() if s == status)
                          for status in (INGESTED, SKIPPED_DUPLICATE, REJECTED_SCHEMA)},
                "rows": dict(result.rows)}

    if stage == "silver":
        result = silver.build(spark, paths, full_refresh=full_refresh)
        return {"stage": stage, "bronze_batches": result.bronze_batches,
                "tables": {name: asdict(t) for name, t in result.tables.items()}}

    if stage == "gold":
        result = gold.publish(spark, paths)
        return {"stage": stage, "publication_id": result.publication_id, "skipped": result.skipped,
                "rows": result.rows, "removed_orphan_rows": result.removed_orphan_rows}

    checks = quality.run(spark, paths)
    counts = {status: sum(c.status == status for c in checks) for status in ("PASS", "WARN", "FAIL")}
    failed = [f"{c.name} {c.target}: {c.observed}" for c in checks if c.status == "FAIL"]
    if failed:
        raise QualityCheckFailed("Controles de calidad en FAIL: " + "; ".join(failed))
    return {"stage": stage, "checks": counts,
            "warnings": [f"{c.name} {c.target}: {c.observed}" for c in checks if c.status == "WARN"]}
