-- Vistas de consumo para Power BI.
-- Ejecutar en el SQL analytics endpoint del Lakehouse (Lakehouse con esquemas habilitados).
--
-- Gold conserva la publicación vigente y la anterior. Estas vistas exponen
-- solo la vigente: un reporte nunca combina filas de dos publicaciones ni ve
-- una publicación sin registrar.
--
-- Si el editor no acepta varios lotes a la vez, ejecuta cada bloque por separado.

CREATE SCHEMA reporting;
GO

CREATE VIEW reporting.current_publication AS
SELECT MAX(publication_id) AS publication_id, MAX(published_at) AS published_at
FROM gold.publications;
GO

CREATE VIEW reporting.dim_cold_room AS
SELECT t.room_code, t.company_code, t.site_name, t.temp_class, t.min_temp_c, t.max_temp_c,
       t.alarm_tolerance_c, t.alarm_low_c, t.alarm_high_c
FROM gold.dim_cold_room AS t
JOIN reporting.current_publication AS p ON t._publication_id = p.publication_id;
GO

CREATE VIEW reporting.dim_product AS
SELECT t.product_code, t.product_name, t.temp_class, t.min_temp_c, t.max_temp_c, t.shelf_life_days
FROM gold.dim_product AS t
JOIN reporting.current_publication AS p ON t._publication_id = p.publication_id;
GO

CREATE VIEW reporting.fact_cold_room_daily AS
SELECT t.room_code, t.reading_date, t.intervals, t.avg_temp_c, t.min_temp_c, t.max_temp_c,
       t.minutes_out_of_range, t.excursion_minutes, t.coverage_pct
FROM gold.fact_cold_room_daily AS t
JOIN reporting.current_publication AS p ON t._publication_id = p.publication_id;
GO

CREATE VIEW reporting.fact_cold_room_excursion AS
SELECT t.room_code, t.company_code, t.temp_class, t.excursion_date, t.start_utc, t.end_utc,
       t.duration_min, t.direction, t.peak_temp_c, t.min_temp_c, t.max_temp_c, t.max_deviation_c, t.severity
FROM gold.fact_cold_room_excursion AS t
JOIN reporting.current_publication AS p ON t._publication_id = p.publication_id;
GO

CREATE VIEW reporting.fact_lot_cold_chain AS
SELECT t.lot_code, t.company_code, t.product_code, t.product_name, t.temp_class, t.production_date,
       t.qty_kg, t.cold_room_code, t.shipment_number, t.in_room_from, t.in_room_until,
       t.room_excursions, t.room_exposure_min, t.critical_exposure_min, t.transit_out_of_range_h,
       t.cold_chain_status
FROM gold.fact_lot_cold_chain AS t
JOIN reporting.current_publication AS p ON t._publication_id = p.publication_id;
GO

CREATE VIEW reporting.fact_container_transit AS
SELECT t.shipment_number, t.company_code, t.container_number, t.carrier_code, t.port_of_loading,
       t.port_of_discharge, t.temp_class, t.planned_departure, t.planned_arrival, t.loaded_at,
       t.departed_at, t.arrived_at, t.latest_eta, t.status, t.arrival_delay_h, t.reefer_readings,
       t.transit_out_of_range_h, t.max_supply_air_c, t.lots, t.qty_kg
FROM gold.fact_container_transit AS t
JOIN reporting.current_publication AS p ON t._publication_id = p.publication_id;
GO

-- Resultado del último control de calidad.
CREATE VIEW reporting.quality_checks AS
SELECT run_at, publication_id, check_name, target, status, observed, expectation
FROM gold.quality_checks
WHERE run_at = (SELECT MAX(run_at) FROM gold.quality_checks);
GO

-- Verificación: debe devolver 6 lotes COMPROMETIDO, 11 EN_RIESGO y 288 OK con todas las llegadas.
SELECT cold_chain_status, COUNT(*) AS lots, SUM(qty_kg) AS qty_kg
FROM reporting.fact_lot_cold_chain
GROUP BY cold_chain_status;
