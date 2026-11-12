"""Contratos de las fuentes: columnas esperadas en cada archivo CSV.

Los eventos de la naviera son JSON Lines y su esquema evoluciona, por lo que
no se validan columnas en la ingesta: Bronze guarda cada línea completa.
"""
from __future__ import annotations

ERP_PRODUCT_COLUMNS = ["product_code", "product_name", "temp_class", "min_temp_c", "max_temp_c",
                       "shelf_life_days"]
ERP_LOT_COLUMNS = ["lot_code", "company_code", "product_code", "production_date", "qty_kg",
                   "cold_room_code", "shipment_number", "updated_at"]
ERP_SHIPMENT_COLUMNS = ["shipment_number", "company_code", "container_number", "carrier_code",
                        "port_of_loading", "port_of_discharge", "temp_class", "planned_departure",
                        "planned_arrival", "updated_at"]
SENSOR_COLUMNS = ["room_code", "sensor_id", "reading_ts", "temperature_c", "humidity_pct"]
