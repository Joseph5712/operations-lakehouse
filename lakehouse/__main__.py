"""Línea de comandos: python -m lakehouse <comando>

  check                  verifica el entorno: escribe, actualiza y lee una tabla Delta con time travel
  generate [--until F]   genera los archivos de origen con fecha de llegada hasta F (AAAA-MM-DD)
  ingest                 ingiere en Bronze los archivos nuevos de la zona de aterrizaje
  silver                 procesa en Silver los lotes de Bronze pendientes
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import date


def check() -> int:
    from .config import paths
    from .spark import get_spark

    spark = get_spark("environment-check")
    target = paths().table("bronze", "_environment_check")
    shutil.rmtree(target, ignore_errors=True)

    print(f"Spark {spark.version}")

    v0 = spark.createDataFrame([("CR01", "PIN-001", 1200), ("GT01", "BAN-001", 800)],
                               "company_code string, product_code string, qty_kg int")
    v0.write.format("delta").mode("overwrite").save(target)

    v1 = spark.createDataFrame([("CR01", "PIN-001", 950), ("GT01", "BAN-001", 800)],
                               "company_code string, product_code string, qty_kg int")
    v1.write.format("delta").mode("overwrite").save(target)

    current = spark.read.format("delta").load(target).agg({"qty_kg": "sum"}).first()[0]
    original = spark.read.format("delta").option("versionAsOf", 0).load(target).agg({"qty_kg": "sum"}).first()[0]
    versions = spark.sql(f"DESCRIBE HISTORY delta.`{target}`").count()

    print(f"Tabla Delta: {target}")
    print(f"  versiones: {versions}")
    print(f"  qty_kg versión actual: {current}")
    print(f"  qty_kg versión 0:      {original}")

    shutil.rmtree(target, ignore_errors=True)
    spark.stop()
    ok = versions == 2 and current == 1750 and original == 2000
    print("Entorno correcto." if ok else "Resultado inesperado.")
    return 0 if ok else 1


def generate(until: date, sensors_per_room: int) -> int:
    from pathlib import Path

    from .config import paths
    from .generator import write_landing
    from .generator.catalog import LAST_ARRIVAL

    landing = Path(paths().files) / "landing"
    summary = write_landing(landing, until=min(until, LAST_ARRIVAL), sensors_per_room=sensors_per_room)
    print(f"Zona de aterrizaje: {landing}  (llegadas hasta {min(until, LAST_ARRIVAL)})")
    print(f"  {'Fuente':<10} {'Nuevos':>7} {'Existentes':>11} {'Filas nuevas':>13}")
    for source in ("erp", "sensors", "carrier"):
        print(f"  {source:<10} {summary.written[source]:>7} {summary.existing[source]:>11} "
              f"{summary.rows[source]:>13,}")
    return 0


def ingest(simulate_failure: bool) -> int:
    from .bronze import INGESTED, REJECTED_SCHEMA, SKIPPED_DUPLICATE, TABLES, SimulatedFailure
    from .bronze import ingest as run_ingest
    from .config import paths
    from .spark import get_spark

    spark = get_spark("bronze-ingest")
    try:
        result = run_ingest(spark, paths(), fail_before_commit=simulate_failure)
    except SimulatedFailure as error:
        print(error)
        spark.stop()
        return 1
    if result.recovered_rows:
        print(f"Recuperación: {result.recovered_rows:,} filas de un lote sin registrar eliminadas")
    if result.batch_id is None:
        print("Sin archivos nuevos.")
        spark.stop()
        return 0
    print(f"Lote {result.batch_id}")
    print(f"  {'Tabla':<17} {'Ingeridos':>9} {'Duplicados':>10} {'Rechazados':>10} {'Filas':>9}")
    for table in TABLES:
        print(f"  {table.name:<17} {result.files[(table.name, INGESTED)]:>9} "
              f"{result.files[(table.name, SKIPPED_DUPLICATE)]:>10} "
              f"{result.files[(table.name, REJECTED_SCHEMA)]:>10} {result.rows[table.name]:>9,}")
    spark.stop()
    return 0


def silver() -> int:
    from .config import paths
    from .silver import build
    from .spark import get_spark

    spark = get_spark("silver")
    result = build(spark, paths())
    if result.bronze_batches is None:
        print("Sin lotes nuevos de Bronze.")
        spark.stop()
        return 0
    first, last = result.bronze_batches
    print(f"Lotes de Bronze procesados: {first}" + (f" a {last}" if last != first else ""))
    print(f"  {'Tabla':<19} {'Leídas':>8} {'Cuarentena':>10} {'Descartadas':>11} {'Insertadas':>10} {'Actualizadas':>12}")
    for name, r in result.tables.items():
        print(f"  {name:<19} {r.bronze_rows:>8,} {r.quarantined:>10,} {r.duplicates:>11,} "
              f"{r.inserted:>10,} {r.updated:>12,}")
    spark.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m lakehouse", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check")
    gen = sub.add_parser("generate")
    gen.add_argument("--until", type=date.fromisoformat, default=date(2026, 11, 3),
                     help="última fecha de llegada a generar (por defecto, todo el periodo)")
    gen.add_argument("--sensors-per-room", type=int, default=2)
    ing = sub.add_parser("ingest")
    ing.add_argument("--simulate-failure", action="store_true",
                     help="interrumpe el lote después de escribir los datos y antes de registrarlo")
    sub.add_parser("silver")
    args = parser.parse_args(argv)
    if args.command == "check":
        return check()
    if args.command == "generate":
        return generate(args.until, args.sensors_per_room)
    if args.command == "ingest":
        return ingest(args.simulate_failure)
    if args.command == "silver":
        return silver()
    return 1


if __name__ == "__main__":
    sys.exit(main())
