import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from lakehouse.generator import write_landing
from lakehouse.generator.catalog import LAST_ARRIVAL
from lakehouse.generator.world import World, iso6346_check_digit, max_temperature


def snapshot(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture(scope="module")
def full_landing(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("full")
    write_landing(root)
    return root


def test_generation_is_deterministic(full_landing, tmp_path):
    write_landing(tmp_path)
    assert snapshot(tmp_path) == snapshot(full_landing)


def test_incremental_generation_matches_full_generation(full_landing, tmp_path):
    first = write_landing(tmp_path, until=date(2026, 10, 13))
    late_files = list(tmp_path.glob("sensors/*/GT01-ESC-01_2026101[012].csv"))
    assert late_files == []          # la pasarela todavía no envió esos días

    second = write_landing(tmp_path, until=LAST_ARRIVAL)
    assert sum(second.existing.values()) == sum(first.written.values())
    assert snapshot(tmp_path) == snapshot(full_landing)


def test_late_sensor_files_arrive_after_outage(full_landing):
    arrivals = {p.parent.name for p in full_landing.glob("sensors/*/GT01-ESC-01_2026101[012].csv")}
    assert arrivals == {"arrival_date=2026-10-14"}


def test_retry_upload_is_byte_identical(full_landing):
    original = full_landing / "sensors/arrival_date=2026-10-20/CR01-LIM-01_20261019.csv"
    retry = full_landing / "sensors/arrival_date=2026-10-21/CR01-LIM-01_20261019_retry1.csv"
    assert original.read_bytes() == retry.read_bytes()


def test_guatemala_gateways_report_local_offset(full_landing):
    gt = (full_landing / "sensors/arrival_date=2026-10-02/GT01-ESC-02_20261001.csv").read_text()
    cr = (full_landing / "sensors/arrival_date=2026-10-02/CR01-ALA-01_20261001.csv").read_text()
    assert "-06:00" in gt.splitlines()[1]
    assert cr.splitlines()[1].split(",")[2].endswith("Z")


def test_carrier_schema_evolves_on_cutover(full_landing):
    def first_event(arrival: str) -> dict:
        path = next(full_landing.glob(f"carrier/arrival_date={arrival}/*.jsonl"))
        return json.loads(path.read_text().splitlines()[0])
    assert "schema_version" not in first_event("2026-10-14")
    assert first_event("2026-10-15")["schema_version"] == 2


def test_carrier_feed_contains_duplicates(full_landing):
    ids = [json.loads(line)["event_id"]
           for path in full_landing.glob("carrier/*/*.jsonl") for line in path.read_text().splitlines()]
    assert len(ids) > len(set(ids))


def test_erp_contains_records_for_quarantine(full_landing):
    lots = "".join(p.read_text() for p in full_landing.glob("erp/*/lots_*.csv"))
    assert ",-500," in lots
    assert "MAN-001" in lots
    shipments = [line.split(",")[2] for p in full_landing.glob("erp/*/shipments_*.csv")
                 for line in p.read_text().splitlines()[1:]]
    invalid = [c for c in shipments if iso6346_check_digit(c[:10]) != int(c[10])]
    assert len(invalid) == 1


def test_iso6346_reference_example():
    assert iso6346_check_digit("CSQU305438") == 3


def test_compressor_failure_breaks_cold_chain():
    world = World()
    assert max_temperature(world.readings[("CR01-LIM-02", date(2026, 10, 7))]) > 16
    assert max_temperature(world.readings[("CR01-LIM-02", date(2026, 10, 6))]) < 16
