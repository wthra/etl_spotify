# Spotify ETL Pipeline

End-to-end batch ETL pipeline ingesting 85,000 Spotify track records through a validated Bronze → Silver → Gold data lakehouse architecture, orchestrated by Apache Airflow and transformed via dbt into a star schema and pre-aggregated gold marts.

**Stack:** Apache Airflow 2.8.1 · PostgreSQL 15 · dbt-postgres 1.7.14 · Docker Compose

---

## Data Architecture

```
+-------------------------------------------------------------------------------------------+
|  LANDING                                                                                  |
|  data/raw_data/spotify_2015_2025_85k.csv                                                  |
|  Raw CSV drop zone — file untouched, never modified by pipeline                           |
+-------------------------------------------------------------------------------------------+
                                        |
                    +-----------------------------------------+
                    |                                         |
                    v                                         v
+-------------------------------+             +-------------------------------+
|  BRONZE                       |             |  DEAD LETTER QUEUE            |
|  data/cleansed/               |             |  data/rejected/               |
|  Validated rows only          |             |  Failed validation rows +     |
|  Normalized dates (ISO 8601)  |             |  error_reason column for      |
|  Temp file — deleted post-run |             |  manual review and replay     |
+-------------------------------+             +-------------------------------+
                |
                v
+-------------------------------------------------------------------------------------------+
|  STAGING  (PostgreSQL — schema: spotify)                                                  |
|  spotify.stg_spotify_tracks                                                               |
|  Full-refresh table: TRUNCATE + COPY from Bronze CSV on every run                        |
+-------------------------------------------------------------------------------------------+
                |
                v
+-------------------------------------------------------------------------------------------+
|  SILVER  (PostgreSQL — schema: spotify, materialized as dbt tables)                       |
|                                                                                           |
|  +------------------+     +------------------+     +---------------------------+          |
|  |   dim_artist     |     |   dim_album      |     |   fact_streams            |          |
|  |  62,391 rows     |     |  84,980 rows     |     |  85,000 rows              |          |
|  |  PK: artist_id   |     |  PK: album_id    |     |  PK: stream_id            |          |
|  |  (surrogate MD5) |     |  (surrogate MD5) |     |  FK: artist_id, album_id  |          |
|  +------------------+     +------------------+     |  Grain: track x country   |          |
|                                                    +---------------------------+          |
+-------------------------------------------------------------------------------------------+
                |
                v
+-------------------------------------------------------------------------------------------+
|  GOLD  (PostgreSQL — schema: spotify_marts, materialized as dbt tables)                   |
|                                                                                           |
|  +------------------------------------+  +------------------------------------------+    |
|  |  daily_top_artists                 |  |  monthly_genre_trends                    |    |
|  |  Artists ranked by stream_count    |  |  Genre rollup with MoM growth %          |    |
|  |  per release_date using RANK()     |  |  and market share using LAG() + window   |    |
|  +------------------------------------+  +------------------------------------------+    |
+-------------------------------------------------------------------------------------------+
```

---

## ETL Pipeline — Task Breakdown

The DAG (`spotify_daily_etl_pipeline`) runs daily at 02:00 UTC with a linear dependency chain:

```
Task 1 → Task 2 → Task 3 → Task 4 → Task 5 → Task 6
```

### Task 1 — extract_and_validate_data

**Operator:** `PythonOperator`

**Why PythonOperator:** Validation logic requires row-by-row iteration with conditional branching, multi-format date parsing, and type coercion — none of which are expressible in SQL or a simple Bash command.

**What it does:**
Reads the raw CSV from `data/raw_data/`, applies 16 validation rules to every row, writes passing rows to `data/cleansed/spotify_cleansed.csv` and failing rows (with an `error_reason` column) to `data/rejected/spotify_rejected.csv`.

**Validation rules applied:**

| Field | Rule |
|---|---|
| track_id | Not null, not empty string |
| track_name | Not null, not empty string |
| artist_name | Not null, not empty string |
| release_date | Parseable in 5 formats; normalized to YYYY-MM-DD; not in future; year >= 1900 |
| duration_ms | Numeric and > 0 |
| popularity | Numeric and in range 0–100 |
| danceability | Numeric and in range 0.0–1.0 |
| energy | Numeric and in range 0.0–1.0 |
| instrumentalness | Numeric and in range 0.0–1.0 |
| loudness | Numeric and in range -60.0 to 0.0 dBFS |
| tempo | Numeric and > 0 |
| key | Integer in range 0–11 |
| mode | Integer 0 or 1 |
| stream_count | Numeric and >= 0 |
| explicit | One of: 0, 1, true, false |

Row counts are pushed to XCom for cross-task assertion in Task 3.

---

### Task 2 — load_cleansed_to_staging

**Operator:** `PythonOperator`

**Why PythonOperator:** Uses `psycopg2` cursor's `copy_expert()` method to issue a PostgreSQL `COPY ... FROM STDIN` command. This bypasses row-by-row INSERT overhead and loads 85k rows in under 2 seconds. The `TRUNCATE` before `COPY` guarantees idempotency — re-running the DAG on the same day always produces the same staging state.

**What it does:**
1. Creates `spotify` schema and `stg_spotify_tracks` table if they do not exist
2. Truncates the table (full refresh)
3. Bulk-loads `spotify_cleansed.csv` via `COPY ... FROM STDIN WITH (FORMAT CSV, HEADER TRUE)`
4. Pushes loaded row count to XCom

---

### Task 3 — data_quality_check

**Operator:** `PythonOperator`

**Why PythonOperator:** SQL checks are executed via `PostgresHook`, and the results are compared against expected values (including the XCom value from Task 1). A Python exception raised here halts the entire pipeline before any dbt models run, ensuring no bad data reaches the star schema.

**What it does:**
Runs 6 SQL assertions against `stg_spotify_tracks`:

| Check | Expected |
|---|---|
| NULL track_ids | 0 |
| Duplicate track_ids | 0 |
| Rows with duration_ms <= 0 | 0 |
| Rows with popularity outside 0–100 | 0 |
| Rows with danceability outside 0–1 | 0 |
| Row count matches Task 1 valid count | Exact match |

The final row count check is a cross-task assertion: it pulls the valid row count from Task 1 via XCom and confirms that exactly that many rows landed in PostgreSQL. This detects silent drops from encoding errors or COPY failures.

---

### Task 4 — transform_to_star_schema

**Operator:** `BashOperator`

**Why BashOperator:** dbt is a CLI tool. BashOperator runs `dbt deps && dbt run` as a subprocess inside the Airflow container where dbt is pre-installed. There is no native Airflow dbt operator that runs faster or provides more control for this use case.

**What it does:**
Runs `dbt run --select +fact_streams` which builds models in dependency order:
1. `stg_tracks_typed` (view over staging table — typed casts + derived columns)
2. `dim_artist` (62,391 unique artists with MD5 surrogate key)
3. `dim_album` (84,980 unique albums deduplicated via `DISTINCT ON`)
4. `fact_streams` (85,000 rows at grain: track × country, with 4 btree indexes)

The `+` prefix in the select expression instructs dbt to include all upstream models automatically.

---

### Task 5 — create_data_marts

**Operator:** `BashOperator`

**Why BashOperator:** Same reason as Task 4 — dbt CLI invocation.

**What it does:**
Runs `dbt run --select +daily_top_artists +monthly_genre_trends` which builds:
1. `daily_top_artists` — artists ranked per `release_date` using `RANK() OVER (PARTITION BY release_date ORDER BY total_streams DESC)`
2. `monthly_genre_trends` — genre streams rolled up by month with `LAG()` for MoM growth and a window-sum for genre share percentage

---

### Task 6 — archive_and_cleanup

**Operator:** `PythonOperator`

**Why PythonOperator:** File system operations (`shutil.move`, `Path.unlink`) and run-date-aware naming (`context["ds_nodash"]`) require Python. The archive filename encodes the run date for traceability.

**What it does:**
1. Moves the source CSV from `data/raw_data/` to `data/archive/spotify_processed_YYYYMMDD.csv`
2. Deletes `data/cleansed/spotify_cleansed.csv` (temp file)
3. Preserves `data/rejected/spotify_rejected.csv` for operator review

**Failure alerting:** Every task has `on_failure_callback` wired to a Slack webhook. If any task fails, Slack receives the DAG name, task name, run ID, and a direct log URL.

---

## Schema Reference

### spotify.stg_spotify_tracks (Staging)

Raw data as loaded — no transformation, no surrogate keys.

| Column | Type | Description |
|---|---|---|
| track_id | TEXT | Spotify track identifier (natural key) |
| track_name | TEXT | Track title |
| artist_name | TEXT | Primary artist name |
| album_name | TEXT | Album title |
| release_date | DATE | Release date, normalized to ISO 8601 |
| genre | TEXT | Music genre tag |
| duration_ms | BIGINT | Track length in milliseconds |
| popularity | SMALLINT | Spotify popularity score (0–100) |
| danceability | NUMERIC(5,3) | Danceability score (0.0–1.0) |
| energy | NUMERIC(5,3) | Energy score (0.0–1.0) |
| key | SMALLINT | Musical key (0=C, 1=C#, ..., 11=B) |
| loudness | NUMERIC(6,2) | Average loudness in dBFS (-60 to 0) |
| mode | SMALLINT | Modality (0=minor, 1=major) |
| instrumentalness | NUMERIC(7,4) | Instrumental probability (0.0–1.0) |
| tempo | NUMERIC(7,2) | Beats per minute |
| stream_count | BIGINT | Cumulative stream count |
| country | TEXT | Market country code |
| explicit | BOOLEAN | Whether track has explicit content |
| label | TEXT | Record label |
| _loaded_at | TIMESTAMPTZ | Timestamp of COPY load |

---

### spotify.dim_artist (Silver)

One row per unique artist. SCD Type 1 — latest values overwrite on each run.

| Column | Type | Description |
|---|---|---|
| artist_id | TEXT | Surrogate key — MD5 hash of artist_name |
| artist_name | TEXT | Artist name (natural key) |
| updated_at | TIMESTAMPTZ | Timestamp of last dbt run |

---

### spotify.dim_album (Silver)

One row per unique (album_name, artist_name) combination. Deduplication uses `DISTINCT ON` ordered by `release_date` to resolve conflicts when the same album appears with different labels or genres across tracks.

| Column | Type | Description |
|---|---|---|
| album_id | TEXT | Surrogate key — MD5 hash of album_name + artist_name |
| artist_id | TEXT | FK to dim_artist |
| album_name | TEXT | Album title |
| artist_name | TEXT | Artist name |
| label | TEXT | Record label |
| genre | TEXT | Genre tag |
| release_date | DATE | Album release date |
| release_year | INT | Derived from release_date |
| updated_at | TIMESTAMPTZ | Timestamp of last dbt run |

---

### spotify.fact_streams (Silver)

Grain: one row per (track_id, country). Measures are additive. Four btree indexes support efficient filtering by date, artist, and album.

| Column | Type | Description |
|---|---|---|
| stream_id | TEXT | Surrogate PK — MD5 hash of track_id + country |
| track_id | TEXT | Natural key from source |
| artist_id | TEXT | FK to dim_artist |
| album_id | TEXT | FK to dim_album |
| track_name | TEXT | Track title |
| country | TEXT | Market country code |
| genre | TEXT | Genre tag |
| explicit | BOOLEAN | Explicit content flag |
| danceability | NUMERIC(5,3) | Audio feature (0.0–1.0) |
| energy | NUMERIC(5,3) | Audio feature (0.0–1.0) |
| key | SMALLINT | Musical key (0–11) |
| loudness | NUMERIC(6,2) | Average loudness in dBFS |
| mode | SMALLINT | Modality (0=minor, 1=major) |
| instrumentalness | NUMERIC(7,4) | Instrumental probability |
| tempo | NUMERIC(7,2) | Beats per minute |
| duration_ms | BIGINT | Track length in milliseconds |
| duration_min | NUMERIC | Track length in minutes (derived) |
| popularity | SMALLINT | Spotify popularity score (0–100) |
| stream_count | BIGINT | Cumulative stream count |
| release_date | DATE | Used as partition key for range filters |
| release_month | DATE | First day of release month (for monthly rollups) |
| release_year | INT | Release year |
| loaded_at | TIMESTAMPTZ | Timestamp of dbt run |

---

### spotify_marts.daily_top_artists (Gold)

Artists ranked by total stream count per release date. Pre-aggregated — no joins required by consumers.

| Column | Type | Description |
|---|---|---|
| release_date | DATE | Date of streams |
| artist_name | TEXT | Artist name |
| artist_id | TEXT | FK to dim_artist |
| total_streams | NUMERIC | Sum of stream_count for this artist on this date |
| avg_popularity | NUMERIC(5,2) | Average popularity across tracks |
| track_count | BIGINT | Number of distinct tracks |
| country_reach | BIGINT | Number of distinct markets |
| rank_by_streams | BIGINT | Rank within date partition (1 = most streamed) |

---

### spotify_marts.monthly_genre_trends (Gold)

Genre performance aggregated by month. Includes month-over-month growth and market share for trend analysis.

| Column | Type | Description |
|---|---|---|
| release_month | DATE | First day of the calendar month |
| release_year | INT | Year |
| genre | TEXT | Genre tag |
| total_streams | NUMERIC | Total streams this genre this month |
| avg_popularity | NUMERIC(5,2) | Average track popularity |
| avg_danceability | NUMERIC(5,3) | Average danceability score |
| avg_energy | NUMERIC(5,3) | Average energy score |
| avg_tempo | NUMERIC(7,2) | Average tempo (BPM) |
| unique_tracks | BIGINT | Distinct track count |
| unique_artists | BIGINT | Distinct artist count |
| market_reach | BIGINT | Distinct country count |
| genre_share_pct | NUMERIC | Share of total monthly streams (sums to 1.0 per month) |
| prev_month_streams | NUMERIC | Previous month's stream count (LAG) |
| mom_growth_pct | NUMERIC | Month-over-month growth rate (%) |

---

## Sample Queries

### Verify star schema referential integrity

```sql
-- FK match rate: dim_artist
SELECT
    COUNT(*)                                        AS fact_rows,
    COUNT(da.artist_id)                             AS matched_artist,
    ROUND(COUNT(da.artist_id) * 100.0 / COUNT(*), 2) AS match_pct
FROM spotify.fact_streams fs
LEFT JOIN spotify.dim_artist da ON fs.artist_id = da.artist_id;

-- Expected: match_pct = 100.00
```

### Query star schema with dim/fact join

```sql
SELECT
    fs.stream_id,
    da.artist_name,
    dal.album_name,
    dal.genre,
    dal.release_year,
    fs.country,
    fs.stream_count,
    fs.popularity,
    ROUND(fs.duration_ms / 60000.0, 2) AS duration_min
FROM spotify.fact_streams  fs
JOIN spotify.dim_artist    da  ON fs.artist_id = da.artist_id
JOIN spotify.dim_album     dal ON fs.album_id  = dal.album_id
ORDER BY fs.stream_count DESC
LIMIT 10;
```

### Top 5 artists per day (Gold layer)

```sql
SELECT
    release_date,
    rank_by_streams   AS rank,
    artist_name,
    total_streams,
    track_count,
    country_reach
FROM spotify_marts.daily_top_artists
WHERE rank_by_streams <= 5
ORDER BY release_date DESC, rank_by_streams
LIMIT 25;
```

### Genre MoM growth trend (Gold layer)

```sql
SELECT
    TO_CHAR(release_month, 'YYYY-MM') AS month,
    genre,
    total_streams,
    ROUND(genre_share_pct * 100, 2)   AS share_pct,
    mom_growth_pct
FROM spotify_marts.monthly_genre_trends
ORDER BY release_month DESC, total_streams DESC
LIMIT 20;
```

### Count by layer (row count verification)

```sql
SELECT 'stg_spotify_tracks' AS table_name, COUNT(*) AS rows FROM spotify.stg_spotify_tracks
UNION ALL
SELECT 'dim_artist',          COUNT(*) FROM spotify.dim_artist
UNION ALL
SELECT 'dim_album',           COUNT(*) FROM spotify.dim_album
UNION ALL
SELECT 'fact_streams',        COUNT(*) FROM spotify.fact_streams
UNION ALL
SELECT 'daily_top_artists',   COUNT(*) FROM spotify_marts.daily_top_artists
UNION ALL
SELECT 'monthly_genre_trends',COUNT(*) FROM spotify_marts.monthly_genre_trends;
```

Run queries in terminal:
```bash
docker exec spotify_postgres psql -U airflow -d airflow -c "<query here>"
```

---

## How to Run

### Prerequisites

- Docker Desktop with WSL2 backend

### Start the stack

```bash
docker-compose up -d --build
```

Wait approximately 60 seconds for `airflow-init` to complete DB migration and create the admin user.

### Drop the source file

```bash
# Place the CSV in the landing zone
cp /path/to/spotify_2015_2025_85k.csv data/raw_data/
```

### Trigger the DAG

Open `http://localhost:8080`, log in as `admin / admin`, navigate to `spotify_daily_etl_pipeline`, and click Trigger.

Alternatively via CLI:
```bash
docker exec spotify_airflow_web airflow dags trigger spotify_daily_etl_pipeline
```

### Check results

```bash
docker exec spotify_postgres psql -U airflow -d airflow -c \
  "SELECT COUNT(*) FROM spotify.fact_streams;"
```

---

## Project Structure

```
ETL_SPOTIFY/
├── dags/
│   └── spotify_etl_dag.py              Main DAG — all 6 tasks
├── dbt_spotify/
│   ├── dbt_project.yml
│   ├── profiles.yml                    DB connection (local dev credentials)
│   ├── packages.yml                    dbt_utils dependency
│   ├── macros/
│   │   └── generate_schema_name.sql    Overrides default dbt schema naming
│   └── models/
│       ├── staging/
│       │   ├── sources.yml             Declares stg_spotify_tracks as dbt source
│       │   └── stg_tracks_typed.sql    Typed view over staging table
│       ├── dimensions/
│       │   ├── dim_artist.sql
│       │   └── dim_album.sql
│       ├── facts/
│       │   └── fact_streams.sql
│       └── marts/
│           ├── daily_top_artists.sql
│           └── monthly_genre_trends.sql
├── data/
│   ├── raw_data/                       Landing zone — place source CSV here
│   ├── cleansed/                       Temp bronze file (deleted post-run)
│   ├── rejected/                       DLQ — invalid rows kept for review
│   └── archive/                        Processed source files (immutable)
├── scripts/
│   └── entrypoint.sh                   Airflow init: db migrate + admin user
├── Dockerfile                          Extends airflow:2.8.1 with dbt + providers
├── docker-compose.yml                  Postgres + Airflow + pgAdmin stack
└── pgadmin_servers.json                Pre-configured pgAdmin server connection
```

---

## Git — What Is and Is Not Committed

| Path | Committed | Reason |
|---|---|---|
| `dags/`, `dbt_spotify/models/`, `dbt_spotify/macros/` | Yes | Source code |
| `Dockerfile`, `docker-compose.yml` | Yes | Infrastructure as code |
| `dbt_spotify/dbt_project.yml`, `packages.yml`, `profiles.yml` | Yes | dbt config (local dev credentials only) |
| `pgadmin_servers.json` | Yes | Local dev server config |
| `data/raw_data/*.csv` | No | Large data file — supply separately |
| `data/archive/*.csv`, `data/rejected/*.csv` | No | Generated outputs |
| `dbt_spotify/dbt_packages/` | No | Installed via `dbt deps` (like node_modules) |
| `dbt_spotify/target/` | No | dbt compiled artifacts |
| `.venv/` | No | Python virtual environment |
| `.claude/` | No | Editor session files |

Note: `profiles.yml` contains a plaintext password for the local PostgreSQL instance. This is acceptable for a local Docker development environment where the same credentials appear in `docker-compose.yml`. Do not use production credentials in this file.
