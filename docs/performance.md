# Rendimiento

Medición del pipeline completo con `python -m lakehouse benchmark`. El volumen de lecturas crece con el número de sensores por cuarto; el resto de las fuentes es igual en ambos casos.

## Entorno

| Concepto | Valor |
|---|---|
| Ejecución | Contenedor Docker del proyecto, Spark en modo `local[*]` |
| CPU | 4 vCPU |
| Memoria del driver | 4 GB (`SPARK_DRIVER_MEMORY=4g`) |
| Versiones | Spark 3.5.3, Delta Lake 3.2.1, Python 3.11 |
| Llegadas | Dos tandas: hasta el 2026-10-13 y el resto |
| Consulta de prueba | Temperatura máxima de un cuarto en un día; mediana de 5 ejecuciones tras una de calentamiento |

## Resultados

| Etapa | 2 sensores/cuarto | 20 sensores/cuarto |
|---|---:|---:|
| Lecturas en Silver | 175,468 | 1,755,009 |
| Generación (todas las llegadas) | 1.5 s | 11.1 s |
| Bronze · carga inicial | 18.4 s | 14.4 s |
| Silver · carga inicial | 33.8 s | 33.8 s |
| Gold · publicación 1 | 22.8 s | 21.1 s |
| Bronze · incremental | 6.5 s | 9.5 s |
| Silver · incremental | 29.4 s | 31.8 s |
| Gold · publicación 2 | 22.5 s | 23.0 s |
| Controles de calidad | 13.9 s | 15.0 s |
| Silver · reproceso completo | 38.8 s | 47.5 s |
| OPTIMIZE de todas las tablas | 38.4 s | 40.3 s |
| Archivos de `silver.cold_room_readings` antes / después de OPTIMIZE | 61 / 61 | 61 / 61 |
| Consulta de un cuarto en un día, antes / después de OPTIMIZE | 0.24 s / 0.33 s | 0.25 s / 0.23 s |

La primera etapa de Spark de la serie de 2 sensores incluye el arranque de la JVM, por eso la carga inicial de Bronze es más lenta que con 20 sensores.

## Interpretación

**Con diez veces más lecturas, los tiempos casi no cambian.** A este volumen, el costo lo domina el trabajo fijo de cada operación (planificar trabajos de Spark, leer el registro de transacciones de Delta y confirmar cada versión), no la cantidad de filas. Solo la generación, que escribe archivos desde Python, crece en proporción al volumen.

**La partición por fecha hace el trabajo de la consulta.** Una consulta de un cuarto en un día lee una sola partición de `reading_date`, con o sin Z-ORDER. Como cada partición diaria ya tiene un solo archivo, OPTIMIZE no tiene nada que compactar en esa tabla y la consulta tarda lo mismo antes y después (las diferencias de centésimas son variación entre ejecuciones).

**OPTIMIZE sí ayuda en Bronze.** Las cargas incrementales dejan varios archivos pequeños por partición de llegada: en la ejecución completa, `bronze.sensor_readings` pasó de 247 a 61 archivos.

## Decisiones

| Decisión | Evidencia | Cuándo revisarla |
|---|---|---|
| Gold se recalcula completo en cada publicación, en lugar de solo las fechas afectadas por datos tardíos | La publicación tarda ~22 s con 176 mil y con 1.76 millones de lecturas; el recálculo parcial agregaría complejidad sin ahorro medible | Cuando la publicación supere varios minutos o crezca en proporción al volumen |
| `silver.cold_room_readings` se particiona por `reading_date` | Las consultas por día leen una sola partición | Si las consultas más frecuentes dejan de filtrar por fecha |
| OPTIMIZE periódico, no después de cada carga | En Silver no reduce archivos ni tiempo de consulta; en Bronze compacta, pero Bronze se lee de forma incremental | Al crecer la cantidad de archivos por partición |
| `spark.sql.shuffle.partitions = 8` en local | El valor por defecto (200) genera tareas y archivos innecesarios para este volumen | En Fabric se usa la configuración del entorno |

En GitHub Codespaces (2 núcleos) los tiempos son mayores; la relación entre volúmenes se mantiene. Para repetir la medición:

```bash
SPARK_DRIVER_MEMORY=4g python -m lakehouse benchmark --sensors-per-room 2 20
```
