"""Escritura de los archivos de origen en la zona de aterrizaje.

Cada archivo se ubica según su fecha de LLEGADA, no según la fecha de sus datos:
  Files/landing/<fuente>/arrival_date=AAAA-MM-DD/<archivo>

Comportamientos de entrega simulados:
  ERP        extracción nocturna: los datos del día D llegan el día D+1
  Sensores   un archivo por cuarto y día, enviado el día siguiente; la pasarela
             GT01-ESC-01 pierde conexión y envía tres días con retraso; un
             reintento reenvía un archivo idéntico con otro nombre; un archivo
             repite sus dos últimas horas
  Naviera    eventos JSON Lines; parte llega un día tarde o repetida; desde
             el 2026-10-15 el emisor publica la versión 2 del esquema
"""
from __future__ import annotations

import csv
import io
import json
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from .catalog import (
    CARRIERS, COLD_ROOMS, GATEWAY_UTC_OFFSET_HOURS, LAST_ARRIVAL, LOCATIONS, PERIOD_END, PERIOD_START,
    PRODUCTS, TEMP_CLASSES,
)
from ..contracts import ERP_LOT_COLUMNS, ERP_PRODUCT_COLUMNS, ERP_SHIPMENT_COLUMNS, SENSOR_COLUMNS
from .world import World, days

SCHEMA_V2_FROM = date(2026, 10, 15)
GATEWAY_OUTAGE = {"room": "GT01-ESC-01", "dates": (date(2026, 10, 10), date(2026, 10, 12)),
                  "arrival": date(2026, 10, 14)}
RETRY_UPLOAD = {"room": "CR01-LIM-01", "reading_date": date(2026, 10, 19), "arrival": date(2026, 10, 21)}
REPEATED_TAIL = {"room": "CR01-ALA-01", "reading_date": date(2026, 9, 15), "hours": 2}



@dataclass(frozen=True)
class LandingFile:
    source: str
    arrival: date
    name: str
    content: bytes
    rows: int


@dataclass
class Summary:
    written: dict = field(default_factory=lambda: defaultdict(int))
    existing: dict = field(default_factory=lambda: defaultdict(int))
    rows: dict = field(default_factory=lambda: defaultdict(int))


def utc(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _csv(columns: list[str], rows: list[dict]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


# --- ERP ---------------------------------------------------------------------------

def _erp_files(world: World) -> list[LandingFile]:
    files = []
    products = [{"product_code": code, "product_name": name, "temp_class": temp_class,
                 "min_temp_c": TEMP_CLASSES[temp_class]["min_c"], "max_temp_c": TEMP_CLASSES[temp_class]["max_c"],
                 "shelf_life_days": shelf}
                for code, (name, temp_class, shelf, _qty) in PRODUCTS.items()]
    files.append(LandingFile("erp", PERIOD_START + timedelta(days=1), "products.csv",
                             _csv(ERP_PRODUCT_COLUMNS, products), len(products)))

    lots_by_day, shipments_by_day = defaultdict(list), defaultdict(list)
    for lot in world.lots:
        base = {"lot_code": lot.code, "company_code": lot.company, "product_code": lot.product,
                "production_date": lot.produced_on.isoformat(), "cold_room_code": lot.room}
        lots_by_day[lot.produced_on].append({**base, "qty_kg": lot.qty_kg, "shipment_number": "",
                                             "updated_at": f"{lot.produced_on}T20:00:00Z"})
        current_qty = lot.qty_kg
        if lot.corrected_on and lot.corrected_on <= PERIOD_END:
            current_qty = lot.corrected_qty_kg
            lots_by_day[lot.corrected_on].append({**base, "qty_kg": current_qty, "shipment_number": "",
                                                  "updated_at": f"{lot.corrected_on}T09:00:00Z"})
        if lot.shipment:
            lots_by_day[lot.loading_date].append({**base, "qty_kg": current_qty, "shipment_number": lot.shipment,
                                                  "updated_at": f"{lot.loading_date}T22:00:00Z"})

    # Registros con defectos que la capa Silver debe enviar a cuarentena.
    lots_by_day[date(2026, 9, 20)].append({
        "lot_code": "CR01-PIN-260920-B", "company_code": "CR01", "product_code": "PIN-001",
        "production_date": "2026-09-20", "qty_kg": -500, "cold_room_code": "CR01-LIM-01",
        "shipment_number": "", "updated_at": "2026-09-20T20:05:00Z"})
    lots_by_day[date(2026, 10, 5)].append({
        "lot_code": "GT01-MAN-261005", "company_code": "GT01", "product_code": "MAN-001",
        "production_date": "2026-10-05", "qty_kg": 6200, "cold_room_code": "GT01-ESC-01",
        "shipment_number": "", "updated_at": "2026-10-05T20:00:00Z"})

    typo_done = False
    for s in world.shipments:
        container = s.container
        if not typo_done and s.booked_on == date(2026, 10, 9):
            # Error de digitación: el dígito de control no corresponde.
            container = container[:-1] + str((int(container[-1]) + 1) % 10)
            typo_done = True
        shipments_by_day[s.booked_on].append({
            "shipment_number": s.number, "company_code": s.company, "container_number": container,
            "carrier_code": s.carrier, "port_of_loading": s.port_of_loading,
            "port_of_discharge": s.port_of_discharge, "temp_class": s.temp_class,
            "planned_departure": utc(s.planned_departure), "planned_arrival": utc(s.planned_arrival),
            "updated_at": f"{s.booked_on}T15:00:00Z"})

    for day in days(PERIOD_START, PERIOD_END):
        arrival = day + timedelta(days=1)
        if lots_by_day[day]:
            files.append(LandingFile("erp", arrival, f"lots_{day:%Y%m%d}.csv",
                                     _csv(ERP_LOT_COLUMNS, lots_by_day[day]), len(lots_by_day[day])))
        if shipments_by_day[day]:
            files.append(LandingFile("erp", arrival, f"shipments_{day:%Y%m%d}.csv",
                                     _csv(ERP_SHIPMENT_COLUMNS, shipments_by_day[day]),
                                     len(shipments_by_day[day])))
    return files


# --- Sensores -------------------------------------------------------------------------

def _sensor_ts(ts: datetime, company: str) -> str:
    offset = GATEWAY_UTC_OFFSET_HOURS[company]
    if offset == 0:
        return utc(ts)
    local = ts + timedelta(hours=offset)
    sign = "+" if offset >= 0 else "-"
    return f"{local:%Y-%m-%dT%H:%M:%S}{sign}{abs(offset):02d}:00"


def _sensor_files(world: World) -> list[LandingFile]:
    files = []
    company_of = {room: company for company, room, _t in COLD_ROOMS}
    for (room, day), readings in sorted(world.readings.items()):
        company = company_of[room]
        rows = [{"room_code": room, "sensor_id": sensor, "reading_ts": _sensor_ts(ts, company),
                 "temperature_c": "" if temp is None else f"{temp:.2f}", "humidity_pct": f"{hum:.1f}"}
                for sensor, ts, temp, hum in readings]
        if room == REPEATED_TAIL["room"] and day == REPEATED_TAIL["reading_date"]:
            tail = 12 * REPEATED_TAIL["hours"] * world.sensors_per_room
            rows += rows[-tail:]

        arrival = day + timedelta(days=1)
        start, end = GATEWAY_OUTAGE["dates"]
        if room == GATEWAY_OUTAGE["room"] and start <= day <= end:
            arrival = GATEWAY_OUTAGE["arrival"]

        name = f"{room}_{day:%Y%m%d}.csv"
        content = _csv(SENSOR_COLUMNS, rows)
        files.append(LandingFile("sensors", arrival, name, content, len(rows)))
        if room == RETRY_UPLOAD["room"] and day == RETRY_UPLOAD["reading_date"]:
            files.append(LandingFile("sensors", RETRY_UPLOAD["arrival"], f"{room}_{day:%Y%m%d}_retry1.csv",
                                     content, len(rows)))
    return files


# --- Naviera ---------------------------------------------------------------------------

def _event_json(event, schema_v2: bool) -> str:
    reefer = None
    if event.reefer:
        reefer = {k: event.reefer[k] for k in ("setpoint_c", "supply_air_c", "return_air_c")}
        if schema_v2:
            reefer["humidity_pct"] = event.reefer["humidity_pct"]
    payload = {}
    if schema_v2:
        payload["schema_version"] = 2
    payload.update({
        "event_id": event.event_id,
        "event_type": event.event_type,
        "event_time": utc(event.event_time),
        "container": {"number": event.container, "iso_type": "45R1"},
        "location": {"locode": event.locode, "name": LOCATIONS[event.locode]} if event.locode else None,
        "vessel": {"name": event.vessel, "voyage": event.voyage} if event.vessel else None,
        "reefer": reefer,
        "eta": utc(event.eta) if event.eta else None,
    })
    return json.dumps(payload, ensure_ascii=False)


def _carrier_files(world: World) -> list[LandingFile]:
    rng = random.Random(7)
    by_arrival = defaultdict(list)
    for event in sorted(world.events, key=lambda e: (e.event_time, e.event_id)):
        arrival = event.event_time.date() + timedelta(days=1)
        roll = rng.random()
        if roll < 0.05:
            arrival += timedelta(days=1)                       # llega tarde y fuera de orden
        by_arrival[arrival].append(event)
        if 0.05 <= roll < 0.07:
            by_arrival[arrival + timedelta(days=1)].append(event)   # reenvío del mismo evento
    files = []
    for arrival, events in sorted(by_arrival.items()):
        v2 = arrival >= SCHEMA_V2_FROM
        lines = "".join(_event_json(e, v2) + "\n" for e in events)
        files.append(LandingFile("carrier", arrival, f"events_{arrival:%Y%m%d}.jsonl",
                                 lines.encode("utf-8"), len(events)))
    return files


# --- Escritura ------------------------------------------------------------------------------

def build_files(world: World) -> list[LandingFile]:
    return _erp_files(world) + _sensor_files(world) + _carrier_files(world)


def write_landing(landing_root: Path, until: date = LAST_ARRIVAL, sensors_per_room: int = 2,
                  seed: int = 2026) -> Summary:
    """Escribe los archivos con llegada hasta `until`. Los existentes no se tocan."""
    summary = Summary()
    for f in build_files(World(seed=seed, sensors_per_room=sensors_per_room)):
        if f.arrival > until:
            continue
        target = Path(landing_root) / f.source / f"arrival_date={f.arrival.isoformat()}" / f.name
        if target.exists():
            summary.existing[f.source] += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(f.content)
        summary.written[f.source] += 1
        summary.rows[f.source] += f.rows
    return summary
