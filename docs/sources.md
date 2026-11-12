# Fuentes de datos

Los datos son sintéticos y deterministas: `python -m lakehouse generate` produce siempre los mismos archivos, byte a byte. El periodo simulado va del 2026-09-01 al 2026-10-31. Los últimos archivos llegan el 2026-11-03.

## Zona de aterrizaje

```
Files/landing/
├── erp/arrival_date=2026-09-02/lots_20260901.csv
├── sensors/arrival_date=2026-09-02/CR01-LIM-01_20260901.csv
└── carrier/arrival_date=2026-09-02/events_20260902.jsonl
```

Los archivos se organizan por **fecha de llegada**, no por la fecha de sus datos. Esto permite simular cargas incrementales: `generate --until 2026-10-13` escribe solo lo que había llegado hasta ese día, y una ejecución posterior agrega lo nuevo sin modificar lo existente.

| Fuente | Archivos | Registros | Volumen |
|---|---:|---:|---:|
| ERP | 119 | 749 | 0.7 MB |
| Sensores | 306 | 176,304 | 9.1 MB |
| Naviera | 59 | 8,729 | 3.4 MB |

## ERP

Extracción nocturna en CSV (UTF-8, encabezado, separador coma). Los datos del día D llegan el día D+1.

### `products.csv`

Maestro de productos. Se entrega una vez, al inicio del periodo.

| Columna | Tipo | Descripción |
|---|---|---|
| `product_code` | texto | Clave del producto |
| `product_name` | texto | Nombre comercial |
| `temp_class` | texto | `FRESH`, `BANANA` o `FROZEN` |
| `min_temp_c` | decimal | Temperatura mínima de conservación |
| `max_temp_c` | decimal | Temperatura máxima de conservación |
| `shelf_life_days` | entero | Vida útil |

| Clase | Rango | Productos |
|---|---|---|
| `FRESH` | 7.0 a 13.0 °C | Piña, papaya |
| `BANANA` | 13.0 a 14.5 °C | Banano |
| `FROZEN` | −25.0 a −18.0 °C | Pulpa de piña, puré de banano |

### `lots_AAAAMMDD.csv`

Altas y cambios de lotes del día. Un mismo lote aparece varias veces durante su vida: al producirse, si se corrige su peso y al cargarse en un contenedor. **La versión vigente es la de mayor `updated_at`.**

| Columna | Tipo | Descripción |
|---|---|---|
| `lot_code` | texto | Clave del lote: `<sociedad>-<producto>-<AAMMDD>` |
| `company_code` | texto | `CR01` o `GT01` |
| `product_code` | texto | Referencia a `products.csv` |
| `production_date` | fecha | Fecha de producción |
| `qty_kg` | entero | Peso del lote |
| `cold_room_code` | texto | Cuarto frío donde se almacena |
| `shipment_number` | texto | Embarque asignado; vacío mientras está en bodega |
| `updated_at` | marca de tiempo UTC | Momento del cambio en el ERP |

### `shipments_AAAAMMDD.csv`

Embarques reservados ese día. Un embarque corresponde a un contenedor refrigerado.

| Columna | Tipo | Descripción |
|---|---|---|
| `shipment_number` | texto | Clave del embarque |
| `company_code` | texto | Sociedad |
| `container_number` | texto | Contenedor ISO 6346: 4 letras, 6 dígitos y dígito de control |
| `carrier_code` | texto | `ATLN` o `CRBR` |
| `port_of_loading` / `port_of_discharge` | texto | UN/LOCODE |
| `temp_class` | texto | Clase de temperatura de la carga |
| `planned_departure` / `planned_arrival` | marca de tiempo UTC | ETD y ETA planificados |
| `updated_at` | marca de tiempo UTC | Momento del cambio en el ERP |

## Sensores de cuartos fríos

Un CSV por cuarto y día, con una lectura cada 5 minutos por sensor. Hay dos sensores por cuarto. Se envía al día siguiente.

| Columna | Tipo | Descripción |
|---|---|---|
| `room_code` | texto | Cuarto frío |
| `sensor_id` | texto | `S1`, `S2` |
| `reading_ts` | marca de tiempo ISO 8601 | Instante de la lectura, **con zona horaria explícita** |
| `temperature_c` | decimal | Temperatura; puede venir vacía |
| `humidity_pct` | decimal | Humedad relativa |

| Cuarto | Clase | Notas |
|---|---|---|
| `CR01-LIM-01`, `CR01-LIM-02` | `FRESH` | Pasarela en UTC (`Z`) |
| `CR01-ALA-01` | `FROZEN` | Pasarela en UTC; descongelamiento a las 02:00, 10:00 y 18:00 UTC (~20 min) |
| `GT01-ESC-01` | `BANANA` | Pasarela en hora local (`-06:00`) |
| `GT01-ESC-02` | `FROZEN` | Pasarela en hora local; descongelamiento programado |

## Naviera

JSON Lines (un evento por línea), en un archivo por día de llegada.

```json
{
  "schema_version": 2,
  "event_id": "25ca9675-0849-532b-98bb-ef121beadc66",
  "event_type": "REEFER_READING",
  "event_time": "2026-10-18T02:00:00Z",
  "container": {"number": "ATLU1271483", "iso_type": "45R1"},
  "location": null,
  "vessel": {"name": "Atlántica Serena", "voyage": "2640E"},
  "reefer": {"setpoint_c": 13.6, "supply_air_c": 14.1, "return_air_c": 15.4, "humidity_pct": 90.0},
  "eta": null
}
```

| `event_type` | Significado | Campos con valor |
|---|---|---|
| `GATE_IN` | Ingreso del contenedor al puerto de carga | `location` |
| `LOADED` | Carga en el buque | `location`, `vessel` |
| `DEPARTED` | Zarpe | `location`, `vessel` |
| `REEFER_READING` | Telemetría del contenedor, cada 6 horas en tránsito | `vessel`, `reefer` |
| `ETA_UPDATED` | Nueva fecha estimada de arribo | `vessel`, `eta` |
| `ARRIVED` | Arribo al puerto de descarga | `location`, `vessel` |
| `DISCHARGED` | Descarga del buque | `location`, `vessel` |
| `GATE_OUT` | Salida del puerto de descarga | `location` |

**Versión 2 del esquema** (archivos con llegada desde el 2026-10-15): agrega `schema_version` y `reefer.humidity_pct`. Los archivos anteriores no traen esos campos.

## Comportamientos conocidos

Comportamientos reales de entrega que el pipeline debe manejar. Los genera el simulador a propósito:

| Fuente | Comportamiento | Detalle | Capa que lo resuelve |
|---|---|---|---|
| Sensores | Archivo reenviado | `CR01-LIM-01_20261019_retry1.csv` es idéntico byte a byte a `CR01-LIM-01_20261019.csv` | Bronze (huella) |
| Sensores | Lecturas repetidas dentro de un archivo | `CR01-ALA-01_20260915.csv` repite sus dos últimas horas | Silver |
| Sensores | Datos tardíos | `GT01-ESC-01` pierde conexión: los días 10 al 12 de octubre llegan el 14 | Silver y Gold |
| Sensores | Zonas horarias mixtas | Guatemala reporta `-06:00`; Costa Rica, UTC | Silver |
| Sensores | Valores inválidos | ~0.04 % de temperaturas vacías y ~0.06 % con el código de error `-99.9` | Silver (cuarentena) |
| Naviera | Eventos repetidos | ~2 % de los eventos se reenvían al día siguiente con el mismo `event_id` | Silver |
| Naviera | Eventos fuera de orden | ~5 % llegan un día tarde | Silver |
| Naviera | Evolución de esquema | Versión 2 desde el 2026-10-15 | Bronze y Silver |
| ERP | Versiones de un mismo lote | Alta, corrección de peso y asignación a embarque | Silver (última versión) |
| ERP | Peso negativo | Lote `CR01-PIN-260920-B` con −500 kg | Silver (cuarentena) |
| ERP | Producto inexistente | Lote `GT01-MAN-261005` con `MAN-001` | Silver (cuarentena) |
| ERP | Contenedor inválido | Un embarque del 2026-10-09 con dígito de control ISO 6346 incorrecto | Silver (cuarentena) |

## Eventos operativos simulados

Situaciones de negocio que Gold debe detectar:

| Evento | Detalle |
|---|---|
| Falla de compresor | `CR01-LIM-02`, 2026-10-07 de 13:00 a 19:30 UTC; la temperatura llega a ~18 °C |
| Aperturas de puerta | 1 a 3 por cuarto y día, de 15 a 55 minutos |
| Falla de contenedor en tránsito | Un embarque fresco de CR01 a Rotterdam, con salida desde el 2026-10-01: la temperatura de suministro sube a partir del tercer día de viaje |
| Retrasos de arribo | ~15 % de los embarques llegan con 1 a 3 días de retraso y emiten `ETA_UPDATED` |
