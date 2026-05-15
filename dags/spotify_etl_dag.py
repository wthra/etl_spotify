"""
Spotify Daily ETL Pipeline
Schedule: every day at 2 AM UTC
"""
import os
import logging
import csv
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ── Paths (inside container) ──────────────────────────────────────────────────
BASE        = Path("/opt/airflow")
RAW_DATA    = BASE / "data" / "raw_data"
CLEANSED    = BASE / "data" / "cleansed"
REJECTED    = BASE / "data" / "rejected"
ARCHIVE     = BASE / "data" / "archive"
DBT_DIR     = BASE / "dbt_spotify"

SOURCE_CSV  = RAW_DATA / "spotify_2015_2025_85k.csv"
CLEANSED_CSV = CLEANSED / "spotify_cleansed.csv"
REJECTED_CSV = REJECTED / "spotify_rejected.csv"

# ── Airflow Variables (override in UI if needed) ──────────────────────────────
DB_CONN_ID       = Variable.get("spotify_db_conn_id", default_var="postgres_default")
SLACK_WEBHOOK_URL = Variable.get("slack_webhook_url", default_var="")


# ── Slack alert callback ──────────────────────────────────────────────────────
def _slack_failure_alert(context):
    """Send Slack notification when any task fails."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("slack_webhook_url Variable not set — skipping Slack alert")
        return

    import urllib.request, json
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    log_url  = context["task_instance"].log_url

    payload = {
        "text": (
            f":red_circle: *ETL Task Failed*\n"
            f">*DAG:* `{dag_id}`\n"
            f">*Task:* `{task_id}`\n"
            f">*Run:* `{run_id}`\n"
            f">*Logs:* <{log_url}|View Logs>"
        )
    }
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10)


# ── DAG default args ──────────────────────────────────────────────────────────
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 14),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _slack_failure_alert,
}

dag = DAG(
    "spotify_daily_etl_pipeline",
    default_args=default_args,
    description="Daily Spotify ETL: ingest → stage → quality → star schema → marts → archive",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["spotify", "etl", "production"],
)


# ============================================================================
# TASK 1 — Extract & Validate
# ============================================================================
def extract_and_validate_data(**context):
    """
    Read spotify_2015_2025_85k.csv from Landing, validate each row,
    write valid rows to cleansed CSV and invalid rows to rejected CSV.

    Validation rules:
      - track_id       : not null / empty
      - duration_ms    : must be > 0
      - popularity     : 0–100
      - danceability   : 0.0–1.0
      - energy         : 0.0–1.0
      - stream_count   : must be >= 0
    """
    CLEANSED.mkdir(parents=True, exist_ok=True)
    REJECTED.mkdir(parents=True, exist_ok=True)

    ingestion_ts = datetime.utcnow().isoformat()

    valid_rows, rejected_rows = [], []

    with open(SOURCE_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            errors = []

            def _float(field, default=-999):
                try:
                    return float(row.get(field, default))
                except (ValueError, TypeError):
                    return None

            # ── Required string fields ────────────────────────────────────────
            if not row.get("track_id", "").strip():
                errors.append("track_id is null or empty")
            if not row.get("track_name", "").strip():
                errors.append("track_name is null or empty")
            if not row.get("artist_name", "").strip():
                errors.append("artist_name is null or empty")

            # ── release_date: try multiple formats, normalize to YYYY-MM-DD ──
            raw_date = row.get("release_date", "").strip()
            DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"]
            parsed_date = None
            for fmt in DATE_FORMATS:
                try:
                    parsed_date = datetime.strptime(raw_date, fmt).date()
                    row["release_date"] = parsed_date.strftime("%Y-%m-%d")  # normalize
                    break
                except (ValueError, TypeError):
                    continue
            if parsed_date is None:
                errors.append(f"release_date unrecognized format: {raw_date}")
            else:
                if parsed_date > datetime.utcnow().date():
                    errors.append(f"release_date is in the future: {raw_date}")
                if parsed_date.year < 1900:
                    errors.append(f"release_date unrealistic: {raw_date}")

            # ── Numeric range checks ──────────────────────────────────────────
            dur = _float("duration_ms")
            if dur is None:
                errors.append("duration_ms is not numeric")
            elif dur <= 0:
                errors.append(f"duration_ms must be > 0: {dur}")

            pop = _float("popularity")
            if pop is None:
                errors.append("popularity is not numeric")
            elif not (0 <= pop <= 100):
                errors.append(f"popularity out of range (0-100): {pop}")

            for field in ("danceability", "energy", "instrumentalness"):
                val = _float(field)
                if val is None:
                    errors.append(f"{field} is not numeric")
                elif not (0.0 <= val <= 1.0):
                    errors.append(f"{field} out of range (0-1): {val}")

            loudness = _float("loudness")
            if loudness is None:
                errors.append("loudness is not numeric")
            elif not (-60.0 <= loudness <= 0.0):
                errors.append(f"loudness out of range (-60 to 0 dBFS): {loudness}")

            tempo = _float("tempo")
            if tempo is None:
                errors.append("tempo is not numeric")
            elif tempo <= 0:
                errors.append(f"tempo must be > 0: {tempo}")

            key_val = _float("key")
            if key_val is None:
                errors.append("key is not numeric")
            elif int(key_val) not in range(0, 12):
                errors.append(f"key must be 0-11: {key_val}")

            mode_val = _float("mode")
            if mode_val is None:
                errors.append("mode is not numeric")
            elif int(mode_val) not in (0, 1):
                errors.append(f"mode must be 0 or 1: {mode_val}")

            sc = _float("stream_count")
            if sc is None:
                errors.append("stream_count is not numeric")
            elif sc < 0:
                errors.append(f"stream_count must be >= 0: {sc}")

            explicit_val = row.get("explicit", "").strip()
            if explicit_val not in ("0", "1", "true", "false", "True", "False"):
                errors.append(f"explicit must be 0 or 1: {explicit_val}")

            if errors:
                row["error_reason"]       = "; ".join(errors)
                row["ingestion_timestamp"] = ingestion_ts
                rejected_rows.append(row)
            else:
                valid_rows.append(row)

    # Write cleansed
    with open(CLEANSED_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(valid_rows)

    # Write rejected
    rejected_fields = list(fieldnames) + ["error_reason", "ingestion_timestamp"]
    with open(REJECTED_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rejected_fields)
        writer.writeheader()
        writer.writerows(rejected_rows)

    stats = {
        "total_rows":    len(valid_rows) + len(rejected_rows),
        "valid_rows":    len(valid_rows),
        "rejected_rows": len(rejected_rows),
        "source_file":   str(SOURCE_CSV),
        "cleansed_file": str(CLEANSED_CSV),
        "rejected_file": str(REJECTED_CSV),
    }
    logger.info(f"Validation stats: {stats}")

    ti = context["task_instance"]
    ti.xcom_push(key="stats",          value=stats)
    ti.xcom_push(key="valid_row_count", value=len(valid_rows))
    return stats


task_1 = PythonOperator(
    task_id="extract_and_validate_data",
    python_callable=extract_and_validate_data,
    dag=dag,
)


# ============================================================================
# TASK 2 — Load Cleansed CSV → Staging Table
# ============================================================================
def load_cleansed_to_staging(**context):
    """
    Bulk-load spotify_cleansed.csv into spotify.stg_spotify_tracks.
    Uses COPY for efficiency; truncates staging before each load.
    """
    import io

    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    try:
        # Ensure schema + table exist
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS spotify;

            CREATE TABLE IF NOT EXISTS spotify.stg_spotify_tracks (
                track_id        TEXT,
                track_name      TEXT,
                artist_name     TEXT,
                album_name      TEXT,
                release_date    DATE,
                genre           TEXT,
                duration_ms     BIGINT,
                popularity      SMALLINT,
                danceability    NUMERIC(5,3),
                energy          NUMERIC(5,3),
                key             SMALLINT,
                loudness        NUMERIC(6,2),
                mode            SMALLINT,
                instrumentalness NUMERIC(7,4),
                tempo           NUMERIC(7,2),
                stream_count    BIGINT,
                country         TEXT,
                explicit        BOOLEAN,
                label           TEXT,
                _loaded_at      TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()

        # Truncate then COPY
        cur.execute("TRUNCATE TABLE spotify.stg_spotify_tracks;")
        conn.commit()

        with open(CLEANSED_CSV, "r", encoding="utf-8") as f:
            # Skip header — COPY handles it via HEADER option
            cur.copy_expert(
                """COPY spotify.stg_spotify_tracks (
                       track_id, track_name, artist_name, album_name,
                       release_date, genre, duration_ms, popularity,
                       danceability, energy, key, loudness, mode,
                       instrumentalness, tempo, stream_count, country,
                       explicit, label
                   )
                   FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')""",
                f,
            )
        conn.commit()

        # Row count for XCom
        cur.execute("SELECT COUNT(*) FROM spotify.stg_spotify_tracks;")
        loaded = cur.fetchone()[0]
        logger.info(f"Loaded {loaded} rows into stg_spotify_tracks")
        context["task_instance"].xcom_push(key="loaded_rows", value=loaded)

    finally:
        cur.close()
        conn.close()


task_2 = PythonOperator(
    task_id="load_cleansed_to_staging",
    python_callable=load_cleansed_to_staging,
    dag=dag,
)


# ============================================================================
# TASK 3 — Data Quality Check
# ============================================================================
def data_quality_check(**context):
    """
    SQL-based DQ checks on stg_spotify_tracks.
    Fails the task (and triggers Slack alert) on first violation.
    """
    ti = context["task_instance"]
    expected_rows = ti.xcom_pull(key="valid_row_count", task_ids="extract_and_validate_data")

    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    checks = {
        "null_track_ids": (
            "SELECT COUNT(*) FROM spotify.stg_spotify_tracks WHERE track_id IS NULL",
            0,
            "Found NULL track_id values",
        ),
        "duplicate_track_ids": (
            """SELECT COUNT(*) FROM (
                   SELECT track_id FROM spotify.stg_spotify_tracks
                   GROUP BY track_id HAVING COUNT(*) > 1
               ) t""",
            0,
            "Found duplicate track_ids",
        ),
        "invalid_duration": (
            "SELECT COUNT(*) FROM spotify.stg_spotify_tracks WHERE duration_ms <= 0",
            0,
            "Found rows with duration_ms <= 0",
        ),
        "invalid_popularity": (
            "SELECT COUNT(*) FROM spotify.stg_spotify_tracks WHERE popularity < 0 OR popularity > 100",
            0,
            "Found rows with popularity outside 0–100",
        ),
        "invalid_danceability": (
            "SELECT COUNT(*) FROM spotify.stg_spotify_tracks WHERE danceability < 0 OR danceability > 1",
            0,
            "Found rows with danceability outside 0–1",
        ),
        "row_count_match": (
            "SELECT COUNT(*) FROM spotify.stg_spotify_tracks",
            expected_rows,
            f"Row count mismatch (expected {expected_rows})",
        ),
    }

    results = {}
    try:
        for name, (query, expected, msg) in checks.items():
            cur.execute(query)
            actual = cur.fetchone()[0]
            results[name] = actual
            logger.info(f"DQ [{name}]: {actual} (expected {expected})")

            if actual != expected:
                raise ValueError(f"DQ FAILED [{name}]: {msg} — got {actual}")

        logger.info("All DQ checks passed!")
        ti.xcom_push(key="dq_results", value=results)

    finally:
        cur.close()
        conn.close()


task_3 = PythonOperator(
    task_id="data_quality_check",
    python_callable=data_quality_check,
    dag=dag,
)


# dbt binary path inside the Airflow container after pip install
DBT_BIN = "/home/airflow/.local/bin/dbt"

# ============================================================================
# TASK 4 — Transform to Star Schema (dbt)
# Builds: dim_artist, dim_album, fact_streams
# ============================================================================
task_4 = BashOperator(
    task_id="transform_to_star_schema",
    bash_command=(
        f"cd {DBT_DIR} && "
        f"{DBT_BIN} deps --profiles-dir . && "
        f"{DBT_BIN} run --profiles-dir . --target dev "
        "--select +fact_streams"
    ),
    dag=dag,
)


# ============================================================================
# TASK 5 — Create Data Marts (dbt)
# Builds: daily_top_artists, monthly_genre_trends
# ============================================================================
task_5 = BashOperator(
    task_id="create_data_marts",
    bash_command=(
        f"cd {DBT_DIR} && "
        f"{DBT_BIN} run --profiles-dir . --target dev "
        "--select +daily_top_artists +monthly_genre_trends"
    ),
    dag=dag,
)


# ============================================================================
# TASK 6 — Archive & Cleanup
# ============================================================================
def archive_and_cleanup(**context):
    """
    Move source CSV from Landing to Archive (renamed with run date).
    Delete cleansed temp file (rejected file is kept for review).
    """
    import shutil

    ARCHIVE.mkdir(parents=True, exist_ok=True)

    run_date    = context["ds_nodash"]          # e.g. 20260515
    archive_dst = ARCHIVE / f"spotify_processed_{run_date}.csv"

    if SOURCE_CSV.exists():
        shutil.move(str(SOURCE_CSV), str(archive_dst))
        logger.info(f"Archived source file → {archive_dst}")
    else:
        logger.warning(f"Source file not found at {SOURCE_CSV} — skipping archive")

    # Remove cleansed temp file
    if CLEANSED_CSV.exists():
        CLEANSED_CSV.unlink()
        logger.info(f"Deleted temp file: {CLEANSED_CSV}")

    result = {
        "archived_to":   str(archive_dst),
        "deleted_files": [str(CLEANSED_CSV)],
    }
    context["task_instance"].xcom_push(key="archive_result", value=result)
    return result


task_6 = PythonOperator(
    task_id="archive_and_cleanup",
    python_callable=archive_and_cleanup,
    dag=dag,
)


# ============================================================================
# DAG Dependencies
# extract → load → quality_check → star_schema → marts → archive
# ============================================================================
task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6
