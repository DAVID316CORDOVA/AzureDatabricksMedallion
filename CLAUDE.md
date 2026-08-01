# Medallion Architecture Practice Project

## Qué es esto

Proyecto de práctica personal para dominar la arquitectura Medallion de extremo a extremo:

```
MySQL (Bronze) → Azure Synapse (Silver) → Databricks + dbt (Gold) → Power BI
```

El objetivo es replicar el stack real que uso en producción (La Tinka / Olimpo Bet) en un entorno local controlado, con datos sucios reales para practicar limpieza, transformación y modelado.

---

## Estado actual

### ✅ Hecho

- **Entorno local**: Python 3.11 venv en `venv/` con `mysql-connector-python` instalado.
- **Base de datos MySQL**: `medallion_bronze` en localhost (separada de `demo_db`).
- **Script de generación de datos sucios**: `scripts/generate_dirty_data.py`
  - Crea las tablas `clientes` y `pedidos` si no existen (sin PK ni FK constraints — intencional).
  - Cada ejecución agrega un **nuevo lote** incremental (`_batch_id` + `_inserted_at`).
  - ~80% datos limpios, ~20% sucios: nulls, duplicados exactos, duplicados de negocio, FK huérfanas, fechas inválidas, formatos inconsistentes, texto corrupto en columnas numéricas.
  - Imprime un resumen de conteos de datos sucios por tipo al final.

### Esquema Bronze (tablas en `medallion_bronze`)

```sql
clientes (id INT, nombre VARCHAR, email VARCHAR, ciudad VARCHAR,
          fecha_registro VARCHAR, _batch_id INT, _inserted_at DATETIME)

pedidos  (id INT, cliente_id INT, producto VARCHAR, cantidad VARCHAR,
          precio_unitario VARCHAR, fecha_pedido VARCHAR, estado VARCHAR,
          _batch_id INT, _inserted_at DATETIME)
```

Nota: columnas numéricas y fechas son `VARCHAR` a propósito — para simular datos sucios reales que llegan de fuentes sin validación.

---

## Cómo correr el generador de datos

```powershell
cd C:\Users\DAVID\Desktop\medallion_practice
.\venv\Scripts\Activate.ps1
python scripts\generate_dirty_data.py
```

Cada ejecución = un día simulado de ingesta Bronze. Corre 3-5 veces para tener volumen suficiente antes de empezar Silver.

---

## Próximos pasos planeados

### Fase 1 — Silver (limpieza y validación)
- Conectar Azure Synapse (o simular con dbt + DuckDB local si Synapse no está disponible).
- Modelos dbt que tomen Bronze y produzcan Silver:
  - `stg_clientes`: strip de espacios, validación regex de email, rango de fechas, deduplicación.
  - `stg_pedidos`: TRY_CAST en cantidad y precio, filtro fechas futuras, flag de FK huérfana.
- Tests dbt: `not_null`, `unique`, `accepted_values`, `relationships`.
- `dbt-expectations`: validar distribuciones, formatos regex, conteos aproximados de filas.

### Fase 2 — Gold (modelado dimensional)
- Fact table: `fct_pedidos` (granularidad: 1 fila por pedido limpio).
- Dim tables: `dim_clientes`, `dim_producto`, `dim_fecha`.
- Métricas calculadas: GMV por ciudad, tasa de cancelación, ticket promedio.

### Fase 3 — Databricks
- Lakeflow / Delta Live Tables reemplazando los modelos dbt de Silver.
- Unity Catalog para gobernanza.
- Autoloader leyendo los CSVs exportados desde MySQL.

### Fase 4 — Observabilidad
- Agregar tabla `_audit_log` que registre conteos de filas y errores por lote.
- Alertas cuando el porcentaje de datos sucios supera un umbral.

---

## Convenciones del proyecto

- Bronze: sin transformaciones, datos tal como llegan, siempre con `_batch_id` y `_inserted_at`.
- Silver: datos limpios, tipados correctamente, sin duplicados, con columna `_dq_flags` para marcar filas con issues.
- Gold: modelos de negocio, completamente agregados y listos para BI/ML.
- Todas las fechas en ISO 8601 (`YYYY-MM-DD`) desde Silver en adelante.
- IDs de negocio nunca se modifican — si hay duda, se flaguea, no se descarta.
