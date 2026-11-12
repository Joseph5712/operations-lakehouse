"""Rutas del lakehouse.

La estructura replica la de un Lakehouse de Microsoft Fabric:
  Files/   archivos tal como llegan de cada fuente (zona de aterrizaje)
  Tables/  tablas Delta, una carpeta por capa

En local la raíz es una carpeta del contenedor. En Fabric será la ruta
abfss:// de OneLake, configurada con la misma variable LAKEHOUSE_ROOT.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class Paths:
    root: str

    @property
    def files(self) -> str:
        return f"{self.root}/Files"

    def landing(self, source: str) -> str:
        return f"{self.files}/landing/{source}"

    def table(self, layer: str, name: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Capa desconocida: {layer}. Usa una de {LAYERS}.")
        return f"{self.root}/Tables/{layer}/{name}"


def paths() -> Paths:
    return Paths(os.getenv("LAKEHOUSE_ROOT", "data").rstrip("/"))
