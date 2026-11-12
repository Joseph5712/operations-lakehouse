"""Simulación determinista de la operación: lotes, embarques, cuartos fríos y tránsito.

La simulación cubre siempre el periodo completo con una misma semilla, de modo
que cualquier subconjunto de archivos generado es idéntico byte a byte al de
una generación completa.
"""
from __future__ import annotations

import math
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

from ..iso6346 import check_digit as iso6346_check_digit
from .catalog import (
    CARRIERS, COLD_ROOMS, COMPANIES, COMPANY_PRODUCTS, CONTAINER_CAPACITY_KG, PERIOD_END,
    PERIOD_START, PRODUCTS, ROUTES, TEMP_CLASSES,
)

UTC = timezone.utc
STEP = timedelta(minutes=5)
EVENT_NAMESPACE = uuid.UUID("6f1c9d4e-2b7a-4c1e-9a55-1d2f3e4a5b6c")

COMPRESSOR_FAILURE = {
    "room": "CR01-LIM-02",
    "start": datetime(2026, 10, 7, 13, 0, tzinfo=UTC),
    "end": datetime(2026, 10, 7, 19, 30, tzinfo=UTC),
    "peak_delta_c": 7.0,
}


def days(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


@dataclass
class Lot:
    code: str
    company: str
    product: str
    produced_on: date
    qty_kg: int
    room: str
    loading_date: date
    shipment: str | None = None
    corrected_on: date | None = None
    corrected_qty_kg: int | None = None


@dataclass
class Shipment:
    number: str
    company: str
    carrier: str
    container: str
    port_of_loading: str
    port_of_discharge: str
    temp_class: str
    booked_on: date
    planned_departure: datetime
    planned_arrival: datetime
    lots: list[str] = field(default_factory=list)
    reefer_failure: bool = False


@dataclass
class CarrierEvent:
    event_id: str
    event_type: str
    event_time: datetime
    container: str
    locode: str
    vessel: str | None
    voyage: str | None
    reefer: dict | None = None
    eta: datetime | None = None


@dataclass
class Disturbance:
    start: datetime
    end: datetime
    peak_delta_c: float
    rise_fraction: float      # proporción del intervalo que tarda en llegar al pico

    def delta_at(self, ts: datetime) -> float:
        if not self.start <= ts < self.end:
            return 0.0
        fraction = (ts - self.start) / (self.end - self.start)
        if fraction < self.rise_fraction:
            return self.peak_delta_c * fraction / self.rise_fraction
        fall = 1 - self.rise_fraction
        return self.peak_delta_c * max(0.0, 1 - (fraction - self.rise_fraction) / fall) ** 0.5


class World:
    def __init__(self, seed: int = 2026, sensors_per_room: int = 2):
        self.rng = random.Random(seed)
        self.sensors_per_room = sensors_per_room
        self.lots: list[Lot] = []
        self.shipments: list[Shipment] = []
        self.events: list[CarrierEvent] = []
        # (cuarto, fecha) -> [(sensor, instante UTC, temperatura, humedad)]
        self.readings: dict[tuple[str, date], list[tuple]] = {}
        self._simulate_lots()
        self._simulate_shipments()
        self._simulate_cold_rooms()
        self._simulate_transit()

    # --- Lotes ------------------------------------------------------------------
    def _simulate_lots(self) -> None:
        rng = self.rng
        for day in days(PERIOD_START, PERIOD_END):
            for company in COMPANIES:
                for product in COMPANY_PRODUCTS[company]:
                    _name, temp_class, _shelf, (low, high) = PRODUCTS[product]
                    rooms = [room for c, room, t in COLD_ROOMS if c == company and t == temp_class]
                    qty = rng.randint(low // 50, high // 50) * 50
                    if temp_class == "FROZEN":
                        # El congelado se consolida y sale los lunes, tras al menos 10 días.
                        earliest = day + timedelta(days=10)
                        loading = earliest + timedelta(days=(7 - earliest.weekday()) % 7)
                    else:
                        loading = day + timedelta(days=rng.randint(2, 4))
                    lot = Lot(f"{company}-{product[:3]}-{day:%y%m%d}", company, product, day,
                              qty, rng.choice(rooms), loading)
                    if rng.random() < 0.04:
                        lot.corrected_on = day + timedelta(days=1)
                        lot.corrected_qty_kg = round(qty * rng.uniform(0.94, 1.06) / 50) * 50
                    self.lots.append(lot)

    # --- Embarques ----------------------------------------------------------------
    def _simulate_shipments(self) -> None:
        rng = self.rng
        groups = defaultdict(list)
        for lot in self.lots:
            if lot.loading_date <= PERIOD_END:
                temp_class = PRODUCTS[lot.product][1]
                groups[(lot.company, temp_class, lot.loading_date)].append(lot)

        counters = defaultdict(int)
        used_containers = set()
        for (company, temp_class, loading), lots in sorted(groups.items()):
            containers, current, load = [], [], 0
            for lot in lots:
                qty = lot.corrected_qty_kg or lot.qty_kg
                if current and load + qty > CONTAINER_CAPACITY_KG:
                    containers.append(current)
                    current, load = [], 0
                current.append(lot)
                load += qty
            containers.append(current)

            for container_lots in containers:
                counters[company] += 1
                port_of_loading, port_of_discharge, transit_days = rng.choice(ROUTES[company])
                carrier = rng.choice(sorted(CARRIERS))
                prefix = CARRIERS[carrier][1]
                while True:
                    serial = f"{prefix}{rng.randint(0, 999_999):06d}"
                    container = f"{serial}{iso6346_check_digit(serial)}"
                    if container not in used_containers:
                        used_containers.add(container)
                        break
                departure = datetime.combine(loading, time(23, 0), UTC)
                shipment = Shipment(
                    number=f"{company}-SHP-{counters[company]:05d}", company=company, carrier=carrier,
                    container=container, port_of_loading=port_of_loading, port_of_discharge=port_of_discharge,
                    temp_class=temp_class, booked_on=max(PERIOD_START, loading - timedelta(days=3)),
                    planned_departure=departure, planned_arrival=departure + timedelta(days=transit_days),
                    lots=[lot.code for lot in container_lots])
                for lot in container_lots:
                    lot.shipment = shipment.number
                self.shipments.append(shipment)

        # Un contenedor refrigerado falla en alta mar: la temperatura sube durante el tránsito.
        failing = next(s for s in self.shipments
                       if s.company == "CR01" and s.temp_class == "FRESH" and s.port_of_discharge == "NLRTM"
                       and s.planned_departure.date() >= date(2026, 10, 1))
        failing.reefer_failure = True

    # --- Cuartos fríos ----------------------------------------------------------------
    def _disturbances(self, room: str, temp_class: str) -> dict[date, list[Disturbance]]:
        rng = self.rng
        by_day: dict[date, list[Disturbance]] = defaultdict(list)

        def add(d: Disturbance) -> None:
            for day in days(d.start.date(), (d.end - timedelta(seconds=1)).date()):
                by_day[day].append(d)

        door_peak = {"FRESH": (2.5, 5.0), "BANANA": (1.0, 2.2), "FROZEN": (5.0, 9.0)}[temp_class]
        for day in days(PERIOD_START, PERIOD_END):
            # Aperturas de puerta durante la carga y descarga.
            for _ in range(rng.choice([1, 2, 2, 3])):
                start = datetime.combine(day, time(0), UTC) + timedelta(minutes=5 * rng.randint(0, 287))
                add(Disturbance(start, start + timedelta(minutes=rng.randint(15, 55)),
                                rng.uniform(*door_peak), 0.3))
            # Los cuartos de congelado descongelan el evaporador tres veces al día.
            if temp_class == "FROZEN":
                for hour in (2, 10, 18):
                    start = datetime.combine(day, time(hour), UTC)
                    add(Disturbance(start, start + timedelta(minutes=20), 5.0, 0.5))
        if room == COMPRESSOR_FAILURE["room"]:
            add(Disturbance(COMPRESSOR_FAILURE["start"], COMPRESSOR_FAILURE["end"] + timedelta(hours=1),
                            COMPRESSOR_FAILURE["peak_delta_c"], 0.45))
        return by_day

    def _simulate_cold_rooms(self) -> None:
        rng = self.rng
        sensors = [f"S{i}" for i in range(1, self.sensors_per_room + 1)]
        for _company, room, temp_class in COLD_ROOMS:
            config = TEMP_CLASSES[temp_class]
            disturbances = self._disturbances(room, temp_class)
            hum_low, hum_high = config["humidity"]
            for day in days(PERIOD_START, PERIOD_END):
                rows = []
                ts = datetime.combine(day, time(0), UTC)
                end = ts + timedelta(days=1)
                active = disturbances.get(day, [])
                while ts < end:
                    delta = sum(d.delta_at(ts) for d in active)
                    for sensor in sensors:
                        temperature = config["setpoint_c"] + delta + rng.gauss(0, 0.2)
                        humidity = rng.uniform(hum_low, hum_high)
                        glitch = rng.random()
                        if glitch < 0.0004:
                            temperature = None                    # lectura vacía
                        elif glitch < 0.0010:
                            temperature = -99.9                   # código de error del sensor
                        rows.append((sensor, ts, temperature, humidity))
                    ts += STEP
                self.readings[(room, day)] = rows

    # --- Tránsito marítimo ----------------------------------------------------------------
    def _simulate_transit(self) -> None:
        rng = self.rng
        horizon = datetime.combine(PERIOD_END + timedelta(days=1), time(0), UTC)
        for s in self.shipments:
            vessel = rng.choice(CARRIERS[s.carrier][2])
            voyage = f"{s.planned_departure:%y}{s.planned_departure.isocalendar().week:02d}{'N' if s.port_of_discharge.startswith('US') else 'E'}"
            setpoint = TEMP_CLASSES[s.temp_class]["setpoint_c"]
            departed = s.planned_departure + timedelta(hours=rng.choice([0, 0, 0, 1, 2, 3, 6, 24]))
            extra_delay = timedelta(hours=rng.choices([0, rng.randint(2, 12), rng.randint(24, 72)],
                                                      weights=[55, 30, 15])[0])
            arrived = s.planned_arrival + (departed - s.planned_departure) + extra_delay

            timeline = [
                ("GATE_IN", s.planned_departure - timedelta(hours=20), s.port_of_loading, False),
                ("LOADED", s.planned_departure - timedelta(hours=4), s.port_of_loading, True),
                ("DEPARTED", departed, s.port_of_loading, True),
            ]
            events = [self._event(s, kind, ts, loc, vessel if on_board else None, voyage if on_board else None)
                      for kind, ts, loc, on_board in timeline]

            reading_ts = departed + timedelta(hours=6)
            while reading_ts < arrived:
                supply = setpoint + rng.gauss(0, 0.3)
                hours_at_sea = (reading_ts - departed).total_seconds() / 3600
                if s.reefer_failure and hours_at_sea > 72:
                    supply += min(7.0, (hours_at_sea - 72) * 0.08)
                reefer = {"setpoint_c": setpoint, "supply_air_c": round(supply, 1),
                          "return_air_c": round(supply + 1.2 + rng.gauss(0, 0.2), 1),
                          "humidity_pct": round(rng.uniform(85, 95), 1)}
                events.append(self._event(s, "REEFER_READING", reading_ts, None, vessel, voyage, reefer=reefer))
                reading_ts += timedelta(hours=6)

            if extra_delay >= timedelta(hours=24):
                notice = departed + (arrived - departed) * 0.6
                events.append(self._event(s, "ETA_UPDATED", notice.replace(minute=0, second=0), None,
                                          vessel, voyage, eta=arrived.replace(minute=0, second=0)))
            events += [
                self._event(s, "ARRIVED", arrived, s.port_of_discharge, vessel, voyage),
                self._event(s, "DISCHARGED", arrived + timedelta(hours=8), s.port_of_discharge, vessel, voyage),
                self._event(s, "GATE_OUT", arrived + timedelta(hours=30), s.port_of_discharge, None, None),
            ]
            self.events += [e for e in events if e.event_time < horizon]

    @staticmethod
    def _event(s: Shipment, kind: str, ts: datetime, locode: str | None, vessel: str | None,
               voyage: str | None, reefer: dict | None = None, eta: datetime | None = None) -> CarrierEvent:
        event_id = str(uuid.uuid5(EVENT_NAMESPACE, f"{s.container}|{kind}|{ts.isoformat()}"))
        return CarrierEvent(event_id, kind, ts, s.container, locode, vessel, voyage, reefer, eta)


def max_temperature(rows: list[tuple]) -> float:
    values = [t for _s, _ts, t, _h in rows if t is not None and t > -99]
    return max(values) if values else math.nan
