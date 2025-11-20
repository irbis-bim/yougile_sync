import psycopg2
from psycopg2.extras import execute_values

SCHEMASQL = """
DROP TABLE IF EXISTS {schema}.task_sprint_stickers CASCADE;
DROP TABLE IF EXISTS {schema}.task_string_stickers CASCADE;
DROP TABLE IF EXISTS {schema}.sprint_sticker_states CASCADE;
DROP TABLE IF EXISTS {schema}.string_sticker_states CASCADE;
DROP TABLE IF EXISTS {schema}.tasks CASCADE;
DROP TABLE IF EXISTS {schema}.users CASCADE;
DROP TABLE IF EXISTS {schema}.sprint_stickers CASCADE;
DROP TABLE IF EXISTS {schema}.string_stickers CASCADE;
DROP TABLE IF EXISTS {schema}.boards CASCADE;

CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.boards (
    id TEXT PRIMARY KEY,
    name TEXT
);

CREATE TABLE IF NOT EXISTS {schema}.users (
    id TEXT PRIMARY KEY,
    name TEXT
);

CREATE TABLE IF NOT EXISTS {schema}.tasks (
    id TEXT PRIMARY KEY,
    title TEXT,
    board_id TEXT REFERENCES {schema}.boards(id) ON DELETE CASCADE,
    assignee_id TEXT REFERENCES {schema}.users(id) ON DELETE SET NULL,
    created_at DATE,
    actual_time DOUBLE PRECISION,
    sprint_name TEXT,
    project_name TEXT,
    direction TEXT,
    state_category TEXT
);
"""

def connect(pg_dsn: str):
    return psycopg2.connect(pg_dsn)

def ensureschema(conn, schema: str = "public"):
    with conn, conn.cursor() as cur:
        cur.execute(SCHEMASQL.format(schema=schema))

def getexistingids(conn, table: str, schema: str = "public") -> set[str]:
    full_table = f"{schema}.{table}"
    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {full_table};")
        return {row[0] for row in cur.fetchall()}

def upsertrows(conn, table: str, columns: list[str], rows: list[tuple], schema: str = "public"):
    if not rows:
        return
    full_table = f"{schema}.{table}"
    cols_str = ", ".join(columns)
    update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != "id"])
    sql = f"""
        INSERT INTO {full_table} ({cols_str})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET {update_cols};
    """
    with conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
