"""
Incrementally replicate the local SQLite `shots` table (written by etl.py)
into BigQuery.

Each run:
  1. reads the high-water mark (max practice_date) from the BigQuery table,
  2. pulls every local row with practice_date > that mark,
  3. loads them into a staging table, and
  4. MERGEs the staging table into the target on the shot's natural key.

Re-reading the watermark day itself means a second session added later the same
day is picked up, and the MERGE keeps reruns idempotent.
"""

import sqlite3
from pathlib import Path

import yaml
from airflow.sdk import dag, task
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DAG_ROOT = Path(__file__).parent
CONFIG_PATH = DAG_ROOT / "config.yml"

with open(CONFIG_PATH) as f:
    cfg = yaml.load(f, yaml.FullLoader)
    DB_NAME = cfg['database_file']
    BQ_PROJECT = cfg['bigquery_project']
    BQ_DATASET = cfg['bigquery_dataset']
    GCP_CONN_ID = cfg.get('gcp_conn_id', 'google_cloud_default')

SOURCE_TABLE = "shots"
TARGET_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{SOURCE_TABLE}"
STAGING_TABLE = f"{TARGET_TABLE}_staging"

# A shot is uniquely identified by its session and its position in the session
KEY_COLUMNS = ["practice_date", "practice_date_index", "shot_index"]
WATERMARK_COLUMN = "practice_date"

SQLITE_TO_BQ_TYPES = {
    "INTEGER": "INT64",
    "BIGINT": "INT64",
    "REAL": "FLOAT64",
    "FLOAT": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "TEXT": "STRING",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_sqlite_schema(conn: sqlite3.Connection) -> list[bigquery.SchemaField]:
    """Build a BigQuery schema from the SQLite table's declared column types."""
    columns = conn.execute(f"PRAGMA table_info({SOURCE_TABLE})").fetchall()
    if not columns:
        raise ValueError(f"Table '{SOURCE_TABLE}' not found in '{DB_NAME}'.")
    return [
        bigquery.SchemaField(name, SQLITE_TO_BQ_TYPES.get(col_type.upper(), "STRING"))
        for _, name, col_type, *_ in columns
    ]


def sync_target_schema(client: bigquery.Client, schema: list[bigquery.SchemaField]) -> None:
    """Create the target table, or add any columns that are new in SQLite."""
    table = bigquery.Table(TARGET_TABLE, schema=schema)
    try:
        existing = client.get_table(TARGET_TABLE)
    except Exception as exc:
        if getattr(exc, "code", None) != 404:
            raise
        client.create_table(table)
        print(f"  → Created {TARGET_TABLE}.")
        return

    known = {field.name for field in existing.schema}
    new_fields = [field for field in schema if field.name not in known]
    if new_fields:
        existing.schema = list(existing.schema) + new_fields
        client.update_table(existing, ["schema"])
        print(f"  → Added columns {[f.name for f in new_fields]} to {TARGET_TABLE}.")


def get_watermark(client: bigquery.Client) -> str | None:
    """Latest practice_date already in BigQuery, or None for an empty table."""
    query = f"SELECT MAX({WATERMARK_COLUMN}) AS watermark FROM `{TARGET_TABLE}`"
    return next(iter(client.query(query).result())).watermark


def extract_rows(conn: sqlite3.Connection, watermark: str | None) -> list[dict]:
    """Rows at or after the watermark (all rows when there is no watermark)."""
    conn.row_factory = sqlite3.Row
    query = f"SELECT * FROM {SOURCE_TABLE}"
    params: tuple = ()
    if watermark is not None:
        query += f" WHERE {WATERMARK_COLUMN} > ?"
        params = (watermark,)
    return [dict(row) for row in conn.execute(query, params)]


def merge_staging_into_target(client: bigquery.Client, columns: list[str]) -> None:
    on_clause = " AND ".join(f"T.{col} = S.{col}" for col in KEY_COLUMNS)
    update_clause = ", ".join(f"{col} = S.{col}" for col in columns if col not in KEY_COLUMNS)
    column_list = ", ".join(columns)
    value_list = ", ".join(f"S.{col}" for col in columns)

    client.query(f"""
        MERGE `{TARGET_TABLE}` T
        USING `{STAGING_TABLE}` S
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({value_list})
    """).result()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id='golf_shots_bigquery_replication',
    # etl.py (golf_launch_monitor_processor) runs at 16:00; run after it
    schedule="30 16 * * *",
    catchup=False,
    max_active_runs=1,
)
def replicate_shots():
    @task(retries=2)
    def replicate():
        client = BigQueryHook(gcp_conn_id=GCP_CONN_ID).get_client(project_id=BQ_PROJECT)

        with sqlite3.connect(DB_NAME) as conn:
            schema = read_sqlite_schema(conn)
            sync_target_schema(client, schema)

            watermark = get_watermark(client)
            print(f"  → Watermark: {watermark}")
            rows = extract_rows(conn, watermark)

        if not rows:
            print("  → No new rows to replicate.")
            return

        client.load_table_from_json(
            rows,
            STAGING_TABLE,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        ).result()

        try:
            merge_staging_into_target(client, [field.name for field in schema])
        finally:
            client.delete_table(STAGING_TABLE, not_found_ok=True)

        print(f"  → Merged {len(rows):,} rows into {TARGET_TABLE}.")

    replicate()


replicate_shots()
