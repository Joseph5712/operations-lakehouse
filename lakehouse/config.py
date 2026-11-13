"""Rutas del lakehouse.

La estructura replica la de un Lakehouse de Microsoft Fabric:
  Files/   archivos tal como llegan de cada fuente (zona de aterrizaje)
  Tables/  tablas Delta, una carpeta por capa (esquemas bronze, silver y gold)

Hay dos formas de llegar a la misma ubicación:
  root     la ruta que usa Spark. En local, una carpeta; en Fabric, la URI
           abfss:// del Lakehouse en OneLake.
  fs_root  la misma ubicación vista como sistema de archivos, para las
           operaciones de Python (listar archivos, calcular huellas). En
           Fabric es el montaje /lakehouse/default; en local coincide con root.

Variables de entorno: LAKEHOUSE_ROOT y, opcionalmente, LAKEHOUSE_FS_ROOT.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class Paths:
    root: str
    fs_root: str | None = None

    @property
    def files(self) -> str:
        return f"{self.root}/Files"

    def landing(self, source: str) -> str:
        return f"{self.files}/landing/{source}"

    def landing_uri(self, relative_path: str) -> str:
        """Ruta Spark de un archivo de la zona de aterrizaje, a partir de su ruta relativa."""
        return f"{self.files}/landing/{relative_path}"

    def table(self, layer: str, name: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Capa desconocida: {layer}. Usa una de {LAYERS}.")
        return f"{self.root}/Tables/{layer}/{name}"

    @property
    def local_root(self) -> Path:
        return Path(self.fs_root or self.root)

    @property
    def landing_dir(self) -> Path:
        """Zona de aterrizaje en el sistema de archivos, para las operaciones de Python."""
        return self.local_root / "Files" / "landing"


def paths() -> Paths:
    root = os.getenv("LAKEHOUSE_ROOT", "data").rstrip("/")
    fs_root = os.getenv("LAKEHOUSE_FS_ROOT")
    return Paths(root, fs_root.rstrip("/") if fs_root else None)


def table_exists(path: str) -> bool:
    """True si la ruta contiene una tabla Delta. Funciona con rutas locales y abfss://."""
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession

    return DeltaTable.isDeltaTable(SparkSession.getActiveSession(), path)
