#!/usr/bin/env python3
"""
Script incremental de datos sucios — Bronze Layer
Proyecto Medallion: MySQL -> Synapse -> Databricks + dbt

Cada ejecucion agrega UN NUEVO LOTE de datos, sin borrar los anteriores.
Simula llegadas diarias para practicar Autoloader / Copy Activity.

Uso: python generate_dirty_data.py
"""

import mysql.connector
import random
from datetime import date, timedelta

# ── CONFIGURACION ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    'host':     'localhost',
    'user':     'root',
    'password': '123',                  # <- ajusta
    'database': 'medallion_bronze'   # base de datos dedicada para la capa Bronze
}

N_CLIENTES = 3000   # filas base por lote
N_PEDIDOS  = 9000   # filas base por lote

# ── CATALOGOS ──────────────────────────────────────────────────────────────────
CIUDADES  = ['Lima', 'Bogota', 'Buenos Aires', 'Santiago', 'Medellin',
             'Quito', 'Caracas', 'Montevideo', 'Ciudad de Mexico', 'Asuncion']
NOMBRES   = ['Carlos Garcia', 'Maria Lopez', 'Juan Rodriguez', 'Ana Martinez',
             'Pedro Gonzalez', 'Laura Sanchez', 'Roberto Diaz', 'Carmen Hernandez',
             'Jose Fernandez', 'Isabel Torres', 'Diego Ramirez', 'Sofia Herrera',
             'Andres Castro', 'Valentina Mora', 'Felipe Ortega']
PRODUCTOS = ['Laptop HP 15', 'Monitor Samsung 27"', 'Teclado Logitech MX',
             'Mouse Razer DeathAdder', 'Auriculares Sony WH-1000XM5',
             'Webcam Logitech C920', 'SSD Kingston 1TB', 'RAM Corsair 16GB',
             'Cable HDMI 4K', 'Hub USB-C 7en1', 'Disco Externo Seagate 2TB',
             'Router TP-Link AX3000', 'Tablet Samsung Tab A8', 'Impresora Epson L3250']
ESTADOS   = ['pendiente', 'procesando', 'enviado', 'entregado', 'cancelado']


# ── HELPERS ────────────────────────────────────────────────────────────────────
def rnd_date(start: date, end: date) -> str:
    return (start + timedelta(days=random.randint(0, (end - start).days))).isoformat()


def rnd_email(nombre: str) -> str:
    dominios = ['gmail.com', 'hotmail.com', 'yahoo.com', 'outlook.com', 'empresa.com']
    base = nombre.lower().replace(' ', '.') \
                 .replace('a','a').replace('e','e')  # simplificado sin tildes
    return f"{base}.{random.randint(1,99)}@{random.choice(dominios)}"


# ── SETUP ──────────────────────────────────────────────────────────────────────
def setup(cursor):
    cursor.execute("SHOW TABLES")
    existing = [t[0] for t in cursor.fetchall()]
    print(f"Tablas existentes antes de ejecutar: {existing}")

    # Sin PRIMARY KEY -> permite duplicados exactos de id
    # Sin FOREIGN KEY -> permite clientes_id huerfanos en pedidos
    # Columnas numericas y fechas como VARCHAR -> permite texto corrupto y formatos mixtos
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id             INT,
            nombre         VARCHAR(200),
            email          VARCHAR(200),
            ciudad         VARCHAR(100),
            fecha_registro VARCHAR(50),
            _batch_id      INT,
            _inserted_at   DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pedidos (
            id              INT,
            cliente_id      INT,
            producto        VARCHAR(300),
            cantidad        VARCHAR(50),
            precio_unitario VARCHAR(50),
            fecha_pedido    VARCHAR(50),
            estado          VARCHAR(50),
            _batch_id       INT,
            _inserted_at    DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Tablas clientes y pedidos listas.")


# ── GENERADOR CLIENTES ─────────────────────────────────────────────────────────
def gen_clientes(cursor, batch_id: int):
    cursor.execute("SELECT COALESCE(MAX(id), 0) FROM clientes")
    nxt = cursor.fetchone()[0] + 1

    REG_INI = date(2022, 1, 1)
    REG_FIN = date(2025, 6, 30)
    HOY     = date.today()

    rows   = []
    stats  = {}

    # Limpios ~80% (16 filas)
    clean_ids    = []
    clean_emails = []
    n_clean = round(N_CLIENTES * 0.80)
    for _ in range(n_clean):
        nom   = random.choice(NOMBRES) + f" {random.randint(100, 9999)}"
        email = rnd_email(nom)
        rows.append((nxt, nom, email, random.choice(CIUDADES),
                     rnd_date(REG_INI, REG_FIN), batch_id))
        clean_ids.append(nxt)
        clean_emails.append(email)
        nxt += 1
    stats['limpios'] = n_clean

    # Nulls: email nulo (2 filas)
    for _ in range(2):
        nom = random.choice(NOMBRES) + f" {random.randint(100, 9999)}"
        rows.append((nxt, nom, None, random.choice(CIUDADES),
                     rnd_date(REG_INI, REG_FIN), batch_id))
        nxt += 1
    stats['email_null'] = 2

    # Duplicados exactos: repetir 2 IDs limpios tal cual
    dup_sources = random.sample(clean_ids, min(2, len(clean_ids)))
    for did in dup_sources:
        orig = next(r for r in rows if r[0] == did)
        rows.append(orig)
    stats['duplicados_exactos'] = len(dup_sources)

    # Duplicado de negocio: mismo email, id distinto
    email_dup = random.choice(clean_emails)
    nom2      = random.choice(NOMBRES) + f" {random.randint(100, 9999)}"
    rows.append((nxt, nom2, email_dup, random.choice(CIUDADES),
                 rnd_date(REG_INI, REG_FIN), batch_id))
    nxt += 1
    stats['duplicado_negocio_email'] = 1

    # Formato incorrecto: espacios extra + MAYUSCULAS + email sin @
    rows.append((nxt,
                 "  " + random.choice(NOMBRES).upper() + "  ",
                 "usuariosinarroba.com",
                 "  " + random.choice(CIUDADES),
                 rnd_date(REG_INI, REG_FIN), batch_id))
    nxt += 1
    stats['formato_nombre_email'] = 1

    # Fecha invalida: muy antigua, en el futuro, o formato raro
    fechas_malas = [
        '1800-03-15',
        (HOY + timedelta(days=500)).isoformat(),
        '99/99/9999',
    ]
    rows.append((nxt,
                 random.choice(NOMBRES) + f" {random.randint(100, 9999)}",
                 rnd_email("test user"),
                 random.choice(CIUDADES),
                 random.choice(fechas_malas), batch_id))
    nxt += 1
    stats['fecha_invalida'] = 1

    stats['total_filas'] = len(rows)
    return rows, stats, clean_ids


# ── GENERADOR PEDIDOS ──────────────────────────────────────────────────────────
def gen_pedidos(cursor, batch_id: int, valid_cids: list):
    cursor.execute("SELECT COALESCE(MAX(id), 0) FROM pedidos")
    nxt = cursor.fetchone()[0] + 1

    REG_INI = date(2022, 1, 1)
    REG_FIN = date(2025, 6, 30)
    HOY     = date.today()

    rows  = []
    stats = {}

    # Limpios ~78% (39 filas)
    n_clean   = round(N_PEDIDOS * 0.78)
    clean_ids = []
    for _ in range(n_clean):
        rows.append((nxt,
                     random.choice(valid_cids),
                     random.choice(PRODUCTOS),
                     str(random.randint(1, 20)),
                     str(round(random.uniform(10.0, 2000.0), 2)),
                     rnd_date(REG_INI, REG_FIN),
                     random.choice(ESTADOS),
                     batch_id))
        clean_ids.append(nxt)
        nxt += 1
    stats['limpios'] = n_clean

    # Nulls: precio_unitario nulo (3 filas)
    for _ in range(3):
        rows.append((nxt, random.choice(valid_cids),
                     random.choice(PRODUCTOS),
                     str(random.randint(1, 10)),
                     None,
                     rnd_date(REG_INI, REG_FIN),
                     random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['precio_null'] = 3

    # FK huerfana: cliente_id que no existe (2 filas)
    for _ in range(2):
        fake_cid = random.randint(99000, 99999)
        rows.append((nxt, fake_cid,
                     random.choice(PRODUCTOS),
                     str(random.randint(1, 5)),
                     str(round(random.uniform(10.0, 500.0), 2)),
                     rnd_date(REG_INI, REG_FIN),
                     random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['fk_huerfana'] = 2

    # Duplicados exactos: repetir 2 pedidos limpios
    dup_pids = random.sample(clean_ids, min(2, len(clean_ids)))
    for dp in dup_pids:
        orig = next(r for r in rows if r[0] == dp)
        rows.append(orig)
    stats['duplicados_exactos'] = len(dup_pids)

    # Fecha en el futuro (2 filas)
    for _ in range(2):
        fecha_fut = (HOY + timedelta(days=random.randint(30, 730))).isoformat()
        rows.append((nxt, random.choice(valid_cids),
                     random.choice(PRODUCTOS),
                     str(random.randint(1, 10)),
                     str(round(random.uniform(10.0, 500.0), 2)),
                     fecha_fut,
                     random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['fecha_futura'] = 2

    # Formato de fecha inconsistente (2 filas)
    fechas_raras = ['15/03/2024', '2024.07.22', 'marzo 2024', '32/13/2023']
    for _ in range(2):
        rows.append((nxt, random.choice(valid_cids),
                     "  " + random.choice(PRODUCTOS).lower() + "  ",
                     str(random.randint(1, 5)),
                     str(round(random.uniform(10.0, 200.0), 2)),
                     random.choice(fechas_raras),
                     random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['fecha_formato_raro'] = 2

    # Cantidad y precio 0 o negativos (2 filas)
    for _ in range(2):
        rows.append((nxt, random.choice(valid_cids),
                     random.choice(PRODUCTOS),
                     str(random.choice([0, -1, -3])),
                     str(round(random.uniform(-500.0, 0), 2)),
                     rnd_date(REG_INI, REG_FIN),
                     random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['numericos_invalidos'] = 2

    # Texto corrupto en cantidad (3 filas)
    textos_corruptos = ['N/A', '#ERROR!', 'null_val', 'ABCD$$', '???']
    for _ in range(3):
        rows.append((nxt, random.choice(valid_cids),
                     random.choice(PRODUCTOS),
                     random.choice(textos_corruptos),   # <-- texto en columna numerica
                     str(round(random.uniform(10.0, 500.0), 2)),
                     rnd_date(REG_INI, REG_FIN),
                     random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['texto_corrupto_cantidad'] = 3

    stats['total_filas'] = len(rows)
    return rows, stats


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    setup(cursor)
    conn.commit()

    # Determinar numero de lote
    cursor.execute("SELECT COALESCE(MAX(_batch_id), 0) + 1 FROM clientes")
    batch_id = cursor.fetchone()[0]
    print(f"\nGenerando LOTE #{batch_id}...")

    # ── Clientes ───────────────────────────────────────────────────────────────
    c_rows, c_stats, new_valid_cids = gen_clientes(cursor, batch_id)

    cursor.executemany(
        "INSERT INTO clientes (id, nombre, email, ciudad, fecha_registro, _batch_id) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        c_rows
    )
    conn.commit()
    print(f"  -> {len(c_rows)} filas insertadas en clientes")

    # Usar TODOS los IDs de clientes existentes para referencias de pedidos
    cursor.execute("SELECT DISTINCT id FROM clientes WHERE id IS NOT NULL")
    all_valid_cids = [r[0] for r in cursor.fetchall()] or new_valid_cids

    # ── Pedidos ────────────────────────────────────────────────────────────────
    p_rows, p_stats = gen_pedidos(cursor, batch_id, all_valid_cids)

    cursor.executemany(
        "INSERT INTO pedidos "
        "(id, cliente_id, producto, cantidad, precio_unitario, fecha_pedido, estado, _batch_id) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        p_rows
    )
    conn.commit()
    print(f"  -> {len(p_rows)} filas insertadas en pedidos")

    cursor.close()
    conn.close()

    # ── Resumen ────────────────────────────────────────────────────────────────
    print(f"""
=================================================================
  RESUMEN LOTE #{batch_id}
=================================================================
  CLIENTES  ({c_stats['total_filas']} filas insertadas)
  -----------------------------------------------------------------
  Limpios                           : {c_stats['limpios']}
  Email NULL                        : {c_stats['email_null']}
  Duplicados exactos (mismo id)     : {c_stats['duplicados_exactos']}
  Duplicado de negocio (mismo email): {c_stats['duplicado_negocio_email']}
  Formato incorrecto (espacios/@)   : {c_stats['formato_nombre_email']}
  Fecha invalida / futura           : {c_stats['fecha_invalida']}
-----------------------------------------------------------------
  PEDIDOS  ({p_stats['total_filas']} filas insertadas)
-----------------------------------------------------------------
  Limpios                           : {p_stats['limpios']}
  precio_unitario NULL              : {p_stats['precio_null']}
  FK huerfana (cliente inexistente) : {p_stats['fk_huerfana']}
  Duplicados exactos                : {p_stats['duplicados_exactos']}
  Fecha futura                      : {p_stats['fecha_futura']}
  Fecha formato raro                : {p_stats['fecha_formato_raro']}
  Cantidad/precio 0 o negativos     : {p_stats['numericos_invalidos']}
  Texto corrupto en cantidad        : {p_stats['texto_corrupto_cantidad']}
=================================================================
  Ejecuta de nuevo para agregar el LOTE #{batch_id + 1}
=================================================================
""")


if __name__ == '__main__':
    main()

# =================================================================
# NOTAS PARA LIMPIEZA EN DBT / DATABRICKS SILVER
# =================================================================
# Por cada lote (~20 clientes, ~50 pedidos) esperas encontrar:
#
# clientes:
#   - 16  filas limpias
#   - 2   con email NULL  -> descartar o imputar
#   - 2   duplicados exactos de id  -> deduplicar por (id + todos los campos)
#   - 1   duplicado de negocio (mismo email, id distinto)  -> regla de negocio
#   - 1   nombre con espacios extra y email sin @  -> strip() + validacion regex
#   - 1   fecha_registro invalida  -> CAST + filtro rango [2020, hoy]
#
# pedidos:
#   - 39  filas limpias
#   - 3   con precio_unitario NULL  -> descartar o flag
#   - 2   FK huerfana (cliente_id no existe en clientes)  -> LEFT JOIN + flag
#   - 2   duplicados exactos  -> deduplicar por (id + todos los campos)
#   - 2   fecha_pedido en el futuro  -> filtro fecha <= hoy
#   - 2   fecha_pedido en formato raro  -> TRY_CAST / regexp
#   - 2   cantidad o precio_unitario <= 0  -> filtro o flag
#   - 3   texto corrupto en cantidad (N/A, #ERROR!, etc.)  -> TRY_CAST -> NULL
# =================================================================
