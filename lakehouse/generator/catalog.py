"""Datos maestros de la operación simulada. Todos los nombres son ficticios."""
from __future__ import annotations

from datetime import date

PERIOD_START = date(2026, 9, 1)
PERIOD_END = date(2026, 10, 31)
# Los archivos más tardíos del periodo llegan unos días después de su cierre.
LAST_ARRIVAL = date(2026, 11, 3)

COMPANIES = {
    "CR01": "AgroExport Costa Rica S.A.",
    "GT01": "AgroExport Guatemala S.A.",
}

# Rango de conservación y punto de ajuste (°C) por clase de temperatura.
TEMP_CLASSES = {
    "FRESH": {"min_c": 7.0, "max_c": 13.0, "setpoint_c": 10.0, "humidity": (88.0, 95.0)},
    "BANANA": {"min_c": 13.0, "max_c": 14.5, "setpoint_c": 13.6, "humidity": (90.0, 95.0)},
    "FROZEN": {"min_c": -25.0, "max_c": -18.0, "setpoint_c": -21.0, "humidity": (70.0, 80.0)},
}

# código: (nombre, clase de temperatura, vida útil en días, rango de kg por lote)
PRODUCTS = {
    "PIN-001": ("Piña MD2", "FRESH", 21, (8_000, 14_000)),
    "PAP-001": ("Papaya Pococí", "FRESH", 14, (5_000, 9_000)),
    "BAN-001": ("Banano Cavendish", "BANANA", 28, (12_000, 20_000)),
    "PUL-001": ("Pulpa de piña congelada", "FROZEN", 540, (4_000, 8_000)),
    "PUR-001": ("Puré de banano congelado", "FROZEN", 365, (4_000, 8_000)),
}

COMPANY_PRODUCTS = {
    "CR01": ["PIN-001", "PAP-001", "PUL-001"],
    "GT01": ["BAN-001", "PUR-001"],
}

# (sociedad, código de cuarto, clase de temperatura)
COLD_ROOMS = [
    ("CR01", "CR01-LIM-01", "FRESH"),
    ("CR01", "CR01-LIM-02", "FRESH"),
    ("CR01", "CR01-ALA-01", "FROZEN"),
    ("GT01", "GT01-ESC-01", "BANANA"),
    ("GT01", "GT01-ESC-02", "FROZEN"),
]

# Las pasarelas de Guatemala reportan hora local con desfase; las de Costa Rica, UTC.
GATEWAY_UTC_OFFSET_HOURS = {"CR01": 0, "GT01": -6}

# código: (nombre, prefijo de contenedor ISO 6346, buques)
CARRIERS = {
    "ATLN": ("Atlántica Line", "ATLU", ["Atlántica Esperanza", "Atlántica Serena"]),
    "CRBR": ("Caribe Reefer", "CRBU", ["Caribe Aurora", "Caribe Marea"]),
}

LOCATIONS = {
    "CRLIO": "Puerto Limón",
    "GTPRQ": "Puerto Quetzal",
    "NLRTM": "Rotterdam",
    "BEANR": "Amberes",
    "DEHAM": "Hamburgo",
    "USPHL": "Filadelfia",
    "USLGB": "Long Beach",
    "KRPUS": "Busan",
}

# (puerto de carga, puerto de descarga, días de tránsito)
ROUTES = {
    "CR01": [("CRLIO", "NLRTM", 14), ("CRLIO", "BEANR", 15), ("CRLIO", "DEHAM", 16), ("CRLIO", "USPHL", 7)],
    "GT01": [("GTPRQ", "USLGB", 7), ("GTPRQ", "KRPUS", 22)],
}

CONTAINER_CAPACITY_KG = 24_000
