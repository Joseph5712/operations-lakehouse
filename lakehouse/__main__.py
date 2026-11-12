"""Línea de comandos: python -m lakehouse <comando>

  check    verifica el entorno: escribe, actualiza y lee una tabla Delta con time travel
"""
from __future__ import annotations

import argparse
import shutil
import sys

from .config import paths
from .spark import get_spark


def check() -> int:
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m lakehouse", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check")
    args = parser.parse_args(argv)
    if args.command == "check":
        return check()
    return 1


if __name__ == "__main__":
    sys.exit(main())
