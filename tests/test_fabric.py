import json
from datetime import date

import pytest

from lakehouse.config import Paths
from lakehouse.fabric import STAGES, QualityCheckFailed, fabric_paths, onelake_root, run_stage


def test_onelake_paths():
    paths = fabric_paths("operations-lakehouse", "lh_operations")
    assert paths.root == "abfss://operations-lakehouse@onelake.dfs.fabric.microsoft.com/lh_operations.Lakehouse"
    assert paths.table("gold", "fact_lot_cold_chain") == f"{paths.root}/Tables/gold/fact_lot_cold_chain"
    assert str(paths.landing_dir) == "/lakehouse/default/Files/landing"
    assert paths.landing_uri("sensors/arrival_date=2026-09-02/x.csv") == \
        f"{onelake_root('operations-lakehouse', 'lh_operations')}/Files/landing/sensors/arrival_date=2026-09-02/x.csv"


def test_all_stages_run_like_a_pipeline(spark, tmp_path):
    # La ruta de Spark y la del sistema de archivos se dan por separado, como en Fabric.
    paths = Paths(root=f"file://{tmp_path}", fs_root=str(tmp_path))
    results = [run_stage(spark, paths, stage, until=date(2026, 9, 10)) for stage in STAGES]
    assert [r["stage"] for r in results] == list(STAGES)
    assert results[1]["batch_id"] == 1 and results[3]["publication_id"] == 1
    assert results[4]["checks"]["FAIL"] == 0
    json.dumps(results, default=str)          # el resumen se puede devolver al pipeline


def test_unknown_stage_is_rejected(spark, tmp_path):
    with pytest.raises(ValueError):
        run_stage(spark, Paths(str(tmp_path)), "platinum")


def test_quality_failures_raise(spark, tmp_path, monkeypatch):
    from lakehouse import quality
    from lakehouse.quality import Check
    monkeypatch.setattr(quality, "run", lambda spark, paths: [Check("clave_unica", "silver.lots", "FAIL", "1", "0")])
    with pytest.raises(QualityCheckFailed):
        run_stage(spark, Paths(str(tmp_path)), "quality")
