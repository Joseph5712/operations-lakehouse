"""Línea de comandos: python -m lakehouse <comando>

  check                  verifica el entorno: escribe, actualiza y lee una tabla Delta con time travel
  generate [--until F]   genera los archivos de origen con fecha de llegada hasta F (AAAA-MM-DD)
  ingest                 ingiere en Bronze los archivos nuevos de la zona de aterrizaje
  silver                 procesa en Silver los lotes de Bronze pendientes
  silver --full-refresh  reconstruye Silver completo desde Bronze
  gold                   publica las tablas Gold y muestra los hallazgos principales
  quality                ejecuta los controles de calidad; termina con error si alguno falla
  pipeline               ejecuta ingest, silver, gold y quality en orden
  maintain [--vacuum]    compacta los archivos de todas las tablas Delta (OPTIMIZE)
  benchmark              mide el pipeline completo con distintos volúmenes
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

    landing = paths().landing_dir
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


def silver(full_refresh: bool = False) -> int:
    from .config import paths
    from .silver import build
    from .spark import get_spark

    spark = get_spark("silver")
    if full_refresh:
        print("Reproceso completo: Silver se reconstruye desde todos los lotes de Bronze.")
    result = build(spark, paths(), full_refresh=full_refresh)
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


def gold(simulate_failure: bool = False) -> int:
    from .config import paths
    from .gold import SimulatedFailure, publish, register_views
    from .spark import get_spark

    spark = get_spark("gold")
    try:
        result = publish(spark, paths(), fail_before_register=simulate_failure)
    except SimulatedFailure as error:
        print(error)
        spark.stop()
        return 1
    if result.removed_orphan_rows:
        print(f"Recuperación: {result.removed_orphan_rows:,} filas de una publicación sin registrar eliminadas")
    if result.skipped:
        print(f"Gold al día: la publicación {result.publication_id} ya incluye todos los datos de Silver.")
    else:
        print(f"Publicación {result.publication_id}")
        for table, rows in result.rows.items():
            print(f"  {table:<26} {rows:>6,} filas")
    _findings(spark)
    spark.stop()
    return 0


def _findings(spark) -> None:
    from .config import paths
    from .gold import register_views

    register_views(spark, paths())
    print("\nExcursiones críticas en cuartos fríos")
    spark.sql("""
        SELECT room_code, start_utc, duration_min, peak_temp_c, max_temp_c
        FROM gold_fact_cold_room_excursion WHERE severity = 'CRITICA'
        ORDER BY duration_min DESC LIMIT 5""").show(truncate=False)
    print("Estado de cadena de frío por lote")
    spark.sql("""
        SELECT cold_chain_status, COUNT(*) AS lots, SUM(qty_kg) AS qty_kg
        FROM gold_fact_lot_cold_chain GROUP BY cold_chain_status ORDER BY cold_chain_status""").show()
    print("Contenedores con temperatura fuera de rango en tránsito")
    spark.sql("""
        SELECT shipment_number, container_number, port_of_discharge, status,
               transit_out_of_range_h, max_supply_air_c, arrival_delay_h
        FROM gold_fact_container_transit WHERE transit_out_of_range_h > 0""").show(truncate=False)


def quality() -> int:
    from .config import paths
    from .quality import run
    from .spark import get_spark

    spark = get_spark("quality")
    checks = run(spark, paths())
    print(f"  {'Estado':<6} {'Control':<23} {'Objeto':<29} Observado")
    for c in checks:
        print(f"  {c.status:<6} {c.name:<23} {c.target:<29} {c.observed}")
    failed = [c for c in checks if c.status == "FAIL"]
    warned = [c for c in checks if c.status == "WARN"]
    print(f"\n{len(checks)} controles: {len(checks) - len(failed) - len(warned)} PASS, "
          f"{len(warned)} WARN, {len(failed)} FAIL")
    spark.stop()
    return 1 if failed else 0


def pipeline() -> int:
    for step in (lambda: ingest(False), silver, gold, quality):
        code = step()
        if code:
            return code
    return 0


def maintain(vacuum: bool) -> int:
    from .config import paths
    from .maintenance import optimize
    from .spark import get_spark

    spark = get_spark("maintenance")
    print(f"  {'Tabla':<34} {'Archivos antes':>14} {'Después':>8} {'Segundos':>9}")
    for r in optimize(spark, paths(), vacuum=vacuum):
        print(f"  {r.table:<34} {r.files_before:>14,} {r.files_after:>8,} {r.seconds:>9.1f}")
    spark.stop()
    return 0


def benchmark(sensors: list[int], root: str) -> int:
    from pathlib import Path

    from .benchmark import run, to_markdown
    from .spark import get_spark

    spark = get_spark("benchmark")
    results = []
    for count in sensors:
        print(f"Midiendo con {count} sensores por cuarto…", flush=True)
        results.append(run(spark, Path(root) / f"sensors_{count}", count))
    print()
    print(to_markdown(results))
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
    slv = sub.add_parser("silver")
    slv.add_argument("--full-refresh", action="store_true", help="reconstruye Silver desde todos los lotes de Bronze")
    gld = sub.add_parser("gold")
    gld.add_argument("--simulate-failure", action="store_true",
                     help="interrumpe la publicación después de escribir las tablas y antes de registrarla")
    sub.add_parser("quality")
    sub.add_parser("pipeline")
    mnt = sub.add_parser("maintain")
    mnt.add_argument("--vacuum", action="store_true", help="borra archivos sin referencia de más de 7 días")
    bench = sub.add_parser("benchmark")
    bench.add_argument("--sensors-per-room", type=int, nargs="+", default=[2, 20])
    bench.add_argument("--root", default="/tmp/lakehouse-benchmark")
    args = parser.parse_args(argv)
    if args.command == "check":
        return check()
    if args.command == "generate":
        return generate(args.until, args.sensors_per_room)
    if args.command == "ingest":
        return ingest(args.simulate_failure)
    if args.command == "silver":
        return silver(args.full_refresh)
    if args.command == "gold":
        return gold(args.simulate_failure)
    if args.command == "quality":
        return quality()
    if args.command == "pipeline":
        return pipeline()
    if args.command == "maintain":
        return maintain(args.vacuum)
    if args.command == "benchmark":
        return benchmark(args.sensors_per_room, args.root)
    return 1


if __name__ == "__main__":
    sys.exit(main())
