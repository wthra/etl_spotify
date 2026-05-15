# Spotify ETL Pipeline

โปรเจกต์ ETL แบบ Batch สำหรับนำเข้าข้อมูลเพลง Spotify 85,000 รายการ ผ่านสถาปัตยกรรม Bronze → Silver → Gold โดยใช้ Apache Airflow ในการ orchestrate และ dbt ในการแปลงข้อมูลเป็น Star Schema และ Gold Mart ที่พร้อมใช้งานสำหรับนักวิเคราะห์ข้อมูล

**Stack:** Apache Airflow 2.8.1 · PostgreSQL 15 · dbt-postgres 1.7.14 · Docker Compose

---

## สถาปัตยกรรมข้อมูล (Data Architecture)

```
+-------------------------------------------------------------------------------------------+
|  LANDING                                                                                  |
|  data/raw_data/spotify_2015_2025_85k.csv                                                  |
|  โซนรับไฟล์ดิบ — ไฟล์ต้นฉบับไม่ถูกแตะต้องหรือแก้ไขโดย pipeline                              |
+-------------------------------------------------------------------------------------------+
                                        |
                    +-----------------------------------------+
                    |                                         |
                    v                                         v
+-------------------------------+             +-------------------------------+
|  BRONZE                       |             |  DEAD LETTER QUEUE (DLQ)      |
|  data/cleansed/               |             |  data/rejected/               |
|  เฉพาะแถวที่ผ่านการตรวจสอบ    |             |  แถวที่ไม่ผ่าน + คอลัมน์      |
|  วันที่ถูก normalize ให้       |             |  error_reason สำหรับตรวจสอบ  |
|  เป็น ISO 8601 ทั้งหมด        |             |  และนำกลับมา replay ใหม่      |
|  ไฟล์ชั่วคราว — ลบหลัง run   |             |                               |
+-------------------------------+             +-------------------------------+
                |
                v
+-------------------------------------------------------------------------------------------+
|  STAGING  (PostgreSQL — schema: spotify)                                                  |
|  spotify.stg_spotify_tracks                                                               |
|  ตาราง buffer สำหรับโหลดข้อมูล: TRUNCATE ก่อน แล้ว COPY จาก Bronze CSV ทุก run           |
+-------------------------------------------------------------------------------------------+
                |
                v
+-------------------------------------------------------------------------------------------+
|  SILVER  (PostgreSQL — schema: spotify, สร้างโดย dbt เป็น materialized table)             |
|                                                                                           |
|  +------------------+     +------------------+     +---------------------------+          |
|  |   dim_artist     |     |   dim_album      |     |   fact_streams            |          |
|  |  62,391 แถว      |     |  84,980 แถว      |     |  85,000 แถว               |          |
|  |  PK: artist_id   |     |  PK: album_id    |     |  PK: stream_id            |          |
|  |  (surrogate MD5) |     |  (surrogate MD5) |     |  FK: artist_id, album_id  |          |
|  +------------------+     +------------------+     |  Grain: track x country   |          |
|                                                    +---------------------------+          |
+-------------------------------------------------------------------------------------------+
                |
                v
+-------------------------------------------------------------------------------------------+
|  GOLD  (PostgreSQL — schema: spotify_marts, สร้างโดย dbt เป็น materialized table)         |
|                                                                                           |
|  +------------------------------------+  +------------------------------------------+    |
|  |  daily_top_artists                 |  |  monthly_genre_trends                    |    |
|  |  จัดอันดับศิลปินตาม stream_count   |  |  สรุปแนวเพลงรายเดือน + MoM growth %     |    |
|  |  แต่ละวัน โดยใช้ RANK()            |  |  และ market share โดยใช้ LAG() + window  |    |
|  +------------------------------------+  +------------------------------------------+    |
+-------------------------------------------------------------------------------------------+
```

---

## Operator ที่ใช้ใน Airflow

Airflow มี Operator หลายประเภทสำหรับงานที่ต่างกัน โปรเจกต์นี้ใช้ 2 ประเภทหลัก:

### PythonOperator

**หลักการทำงาน:** รันฟังก์ชัน Python ใดก็ได้ที่กำหนดไว้ภายใน DAG โดยตรง Airflow จะ pass `context` dictionary เข้าไปในฟังก์ชัน ทำให้เข้าถึง metadata ของ run เช่น run date, task instance, XCom ได้

**ใช้กับ data อย่างไร:**
- อ่านและเขียนไฟล์ CSV ผ่าน Python standard library (`csv.DictReader`, `csv.DictWriter`)
- เชื่อมต่อ PostgreSQL ผ่าน `PostgresHook` แล้วใช้ `cursor.copy_expert()` สำหรับ bulk load
- ส่งผลลัพธ์ระหว่าง task ผ่าน XCom (`ti.xcom_push` / `ti.xcom_pull`)
- จัดการไฟล์ระบบ เช่น ย้ายไฟล์, ลบไฟล์ผ่าน `shutil` และ `pathlib`

**เลือกใช้เมื่อ:** logic ต้องการการตัดสินใจหลายเงื่อนไข (if/else), วนลูป, แปลง type, หรือใช้ Python library ที่ SQL และ Bash ทำไม่ได้

**ใช้ใน Task:** 1, 2, 3, 6

---

### BashOperator

**หลักการทำงาน:** รัน shell command ภายใน Airflow container โดยตรง เหมาะสำหรับเรียกใช้ CLI tool ที่ติดตั้งไว้ในระบบ

**ใช้กับ data อย่างไร:**
- เรียก `dbt run` ซึ่งเป็น CLI ที่อ่าน SQL model files, compile เป็น query, แล้วรันกับ PostgreSQL
- dbt เชื่อมต่อกับฐานข้อมูลผ่าน `profiles.yml` และสร้าง table ใน schema ที่กำหนด
- ผลลัพธ์คือตารางใหม่ใน PostgreSQL (dim_artist, dim_album, fact_streams, daily_top_artists, monthly_genre_trends)
- BashOperator จะ fail task ทันทีถ้า dbt คืน exit code ที่ไม่ใช่ 0 ทำให้ pipeline หยุดอัตโนมัติ

**เลือกใช้เมื่อ:** ต้องการรัน external CLI tool ที่ติดตั้งไว้ใน container อยู่แล้ว ไม่มี native Airflow operator รองรับ

**ใช้ใน Task:** 4, 5

---

## รายละเอียด Task ทั้ง 6

DAG `spotify_daily_etl_pipeline` รันทุกวันเวลา 02:00 UTC ด้วย dependency แบบเส้นตรง:

```
Task 1 → Task 2 → Task 3 → Task 4 → Task 5 → Task 6
```

### Task 1 — extract_and_validate_data (PythonOperator)

อ่านไฟล์ CSV จาก `data/raw_data/` ตรวจสอบทุกแถวด้วย 15 กฎ แถวที่ผ่านเขียนไปที่ `data/cleansed/` แถวที่ไม่ผ่านเขียนไปที่ `data/rejected/` พร้อมคอลัมน์ `error_reason`

กฎที่ตรวจสอบ:

| Field | เงื่อนไข |
|---|---|
| track_id | ห้าม null หรือว่างเปล่า |
| track_name | ห้าม null หรือว่างเปล่า |
| artist_name | ห้าม null หรือว่างเปล่า |
| release_date | รองรับ 5 format, normalize เป็น YYYY-MM-DD, ห้ามเป็นวันในอนาคต, ปี >= 1900 |
| duration_ms | ต้องเป็นตัวเลขและ > 0 |
| popularity | ต้องเป็นตัวเลขในช่วง 0–100 |
| danceability | ต้องเป็นตัวเลขในช่วง 0.0–1.0 |
| energy | ต้องเป็นตัวเลขในช่วง 0.0–1.0 |
| instrumentalness | ต้องเป็นตัวเลขในช่วง 0.0–1.0 |
| loudness | ต้องเป็นตัวเลขในช่วง -60.0 ถึง 0.0 dBFS |
| tempo | ต้องเป็นตัวเลขและ > 0 |
| key | ต้องเป็น integer ในช่วง 0–11 |
| mode | ต้องเป็น 0 หรือ 1 เท่านั้น |
| stream_count | ต้องเป็นตัวเลขและ >= 0 |
| explicit | ต้องเป็นหนึ่งใน: 0, 1, true, false |

จำนวนแถวที่ผ่านถูก push ไปยัง XCom เพื่อใช้ตรวจสอบใน Task 3

---

### Task 2 — load_cleansed_to_staging (PythonOperator)

รับไฟล์จาก Bronze layer แล้วโหลดเข้า PostgreSQL โดยใช้คำสั่ง `COPY` ซึ่งเร็วกว่า INSERT ธรรมดามากสำหรับข้อมูลจำนวนมาก

ขั้นตอน:
1. สร้าง schema `spotify` และตาราง `stg_spotify_tracks` ถ้ายังไม่มี
2. `TRUNCATE` ตารางทิ้ง (full refresh ทุก run เพื่อ idempotency)
3. โหลด CSV ด้วย `COPY ... FROM STDIN WITH (FORMAT CSV, HEADER TRUE)` ผ่าน `cursor.copy_expert()`
4. Push จำนวนแถวที่โหลดไปยัง XCom

---

### Task 3 — data_quality_check (PythonOperator)

รัน 6 SQL assertion เพื่อตรวจสอบคุณภาพข้อมูลใน staging table ก่อนที่ dbt จะทำงาน ถ้าเช็คใดไม่ผ่านจะ raise `ValueError` ทำให้ pipeline หยุดทันที

| การตรวจสอบ | ค่าที่คาดหวัง |
|---|---|
| track_id ที่เป็น NULL | 0 |
| track_id ที่ซ้ำกัน | 0 |
| แถวที่มี duration_ms <= 0 | 0 |
| แถวที่มี popularity นอกช่วง 0–100 | 0 |
| แถวที่มี danceability นอกช่วง 0–1 | 0 |
| จำนวนแถวตรงกับ Task 1 (cross-task assertion) | ต้องเท่ากันพอดี |

การตรวจสอบสุดท้ายดึงค่าจาก XCom ของ Task 1 มาเปรียบเทียบกับจำนวนแถวใน PostgreSQL เพื่อตรวจจับการสูญหายของข้อมูลระหว่างขั้นตอน CSV → Database

---

### Task 4 — transform_to_star_schema (BashOperator)

รัน `dbt run --select +fact_streams` ซึ่ง dbt จะสร้าง model ตามลำดับ dependency:

1. `stg_tracks_typed` — view ที่ cast type และเพิ่มคอลัมน์ derived เช่น `duration_min`, `release_month`
2. `dim_artist` — 62,391 artist ไม่ซ้ำ พร้อม surrogate key จาก MD5 hash
3. `dim_album` — 84,980 album ไม่ซ้ำ deduplicate ด้วย `DISTINCT ON (album_name, artist_name)`
4. `fact_streams` — 85,000 แถว grain คือ track × country มี 4 btree index สำหรับ query performance

เครื่องหมาย `+` หน้าชื่อ model หมายถึงให้ dbt รัน upstream model ทั้งหมดโดยอัตโนมัติ

---

### Task 5 — create_data_marts (BashOperator)

รัน `dbt run --select +daily_top_artists +monthly_genre_trends` เพื่อสร้าง Gold layer:

1. `daily_top_artists` — สรุปยอด stream รายวันต่อศิลปิน จัดอันดับด้วย `RANK() OVER (PARTITION BY release_date ORDER BY total_streams DESC)`
2. `monthly_genre_trends` — สรุปแนวเพลงรายเดือน คำนวณ MoM growth ด้วย `LAG()` และ genre share ด้วย window sum

---

### Task 6 — archive_and_cleanup (PythonOperator)

จัดการไฟล์หลัง pipeline เสร็จสิ้น:

1. ย้ายไฟล์ต้นฉบับจาก `data/raw_data/` ไปที่ `data/archive/spotify_processed_YYYYMMDD.csv` (ชื่อไฟล์มี run date)
2. ลบไฟล์ชั่วคราว `data/cleansed/spotify_cleansed.csv`
3. เก็บ `data/rejected/spotify_rejected.csv` ไว้สำหรับ operator ตรวจสอบ

**การแจ้งเตือนเมื่อเกิด error:** ทุก task มี `on_failure_callback` เชื่อมกับ Slack webhook ถ้า task ใดล้มเหลวจะส่ง alert พร้อมชื่อ DAG, ชื่อ task, run ID และลิงก์ log ทันที

---

## Schema Reference

### spotify.stg_spotify_tracks (Staging)

ข้อมูลดิบที่โหลดเข้ามา ยังไม่มี transformation หรือ surrogate key

| คอลัมน์ | ประเภท | คำอธิบาย |
|---|---|---|
| track_id | TEXT | Spotify track identifier (natural key) |
| track_name | TEXT | ชื่อเพลง |
| artist_name | TEXT | ชื่อศิลปินหลัก |
| album_name | TEXT | ชื่ออัลบั้ม |
| release_date | DATE | วันที่วางจำหน่าย normalize แล้วเป็น ISO 8601 |
| genre | TEXT | แนวเพลง |
| duration_ms | BIGINT | ความยาวเพลงหน่วยมิลลิวินาที |
| popularity | SMALLINT | คะแนนความนิยม Spotify (0–100) |
| danceability | NUMERIC(5,3) | ความสามารถในการเต้น (0.0–1.0) |
| energy | NUMERIC(5,3) | ระดับพลังงานของเพลง (0.0–1.0) |
| key | SMALLINT | คีย์ดนตรี (0=C, 1=C#, ..., 11=B) |
| loudness | NUMERIC(6,2) | ความดังเฉลี่ย dBFS (-60 ถึง 0) |
| mode | SMALLINT | โหมดดนตรี (0=minor, 1=major) |
| instrumentalness | NUMERIC(7,4) | ความน่าจะเป็นที่เป็นเพลงบรรเลง (0.0–1.0) |
| tempo | NUMERIC(7,2) | จังหวะ beats per minute |
| stream_count | BIGINT | จำนวนการสตรีมสะสม |
| country | TEXT | รหัสประเทศตลาด |
| explicit | BOOLEAN | มีเนื้อหาผู้ใหญ่หรือไม่ |
| label | TEXT | ค่ายเพลง |
| _loaded_at | TIMESTAMPTZ | เวลาที่โหลดเข้าผ่าน COPY |

---

### spotify.dim_artist (Silver)

หนึ่งแถวต่อหนึ่งศิลปินไม่ซ้ำ SCD Type 1 — ค่าล่าสุดทับของเก่าทุก run

| คอลัมน์ | ประเภท | คำอธิบาย |
|---|---|---|
| artist_id | TEXT | Surrogate key — MD5 hash ของ artist_name |
| artist_name | TEXT | ชื่อศิลปิน (natural key) |
| updated_at | TIMESTAMPTZ | เวลา dbt run ล่าสุด |

---

### spotify.dim_album (Silver)

หนึ่งแถวต่อคู่ (album_name, artist_name) ไม่ซ้ำ ใช้ `DISTINCT ON` เพื่อแก้ปัญหาอัลบั้มเดียวกันที่มี label หรือ genre ต่างกันในข้อมูลต้นฉบับ

| คอลัมน์ | ประเภท | คำอธิบาย |
|---|---|---|
| album_id | TEXT | Surrogate key — MD5 hash ของ album_name + artist_name |
| artist_id | TEXT | FK ไปยัง dim_artist |
| album_name | TEXT | ชื่ออัลบั้ม |
| artist_name | TEXT | ชื่อศิลปิน |
| label | TEXT | ค่ายเพลง |
| genre | TEXT | แนวเพลง |
| release_date | DATE | วันที่วางจำหน่ายอัลบั้ม |
| release_year | INT | ปีที่วางจำหน่าย (derived จาก release_date) |
| updated_at | TIMESTAMPTZ | เวลา dbt run ล่าสุด |

---

### spotify.fact_streams (Silver)

Grain: หนึ่งแถวต่อ (track_id, country) measure ทุกตัวเป็น additive มี 4 btree index สำหรับ filter ตาม date, artist, album

| คอลัมน์ | ประเภท | คำอธิบาย |
|---|---|---|
| stream_id | TEXT | Surrogate PK — MD5 hash ของ track_id + country |
| track_id | TEXT | Natural key จาก source |
| artist_id | TEXT | FK ไปยัง dim_artist |
| album_id | TEXT | FK ไปยัง dim_album |
| track_name | TEXT | ชื่อเพลง |
| country | TEXT | รหัสประเทศตลาด |
| genre | TEXT | แนวเพลง |
| explicit | BOOLEAN | มีเนื้อหาผู้ใหญ่หรือไม่ |
| danceability | NUMERIC(5,3) | Audio feature (0.0–1.0) |
| energy | NUMERIC(5,3) | Audio feature (0.0–1.0) |
| key | SMALLINT | คีย์ดนตรี (0–11) |
| loudness | NUMERIC(6,2) | ความดังเฉลี่ย dBFS |
| mode | SMALLINT | โหมดดนตรี (0=minor, 1=major) |
| instrumentalness | NUMERIC(7,4) | ความน่าจะเป็นที่เป็นเพลงบรรเลง |
| tempo | NUMERIC(7,2) | Beats per minute |
| duration_ms | BIGINT | ความยาวเพลงหน่วยมิลลิวินาที |
| duration_min | NUMERIC | ความยาวเพลงหน่วยนาที (derived) |
| popularity | SMALLINT | คะแนนความนิยม (0–100) |
| stream_count | BIGINT | จำนวนการสตรีมสะสม |
| release_date | DATE | ใช้เป็น partition key สำหรับ filter ตาม date range |
| release_month | DATE | วันแรกของเดือน (สำหรับ monthly rollup) |
| release_year | INT | ปีที่วางจำหน่าย |
| loaded_at | TIMESTAMPTZ | เวลา dbt run |

---

### spotify_marts.daily_top_artists (Gold)

จัดอันดับศิลปินตามยอด stream รายวัน pre-aggregated ไม่ต้อง join เพิ่มเติม

| คอลัมน์ | ประเภท | คำอธิบาย |
|---|---|---|
| release_date | DATE | วันที่ |
| artist_name | TEXT | ชื่อศิลปิน |
| artist_id | TEXT | FK ไปยัง dim_artist |
| total_streams | NUMERIC | รวม stream_count ของศิลปินในวันนั้น |
| avg_popularity | NUMERIC(5,2) | ค่าเฉลี่ย popularity ของเพลงทั้งหมด |
| track_count | BIGINT | จำนวนเพลงที่แตกต่างกัน |
| country_reach | BIGINT | จำนวนประเทศที่มีการสตรีม |
| rank_by_streams | BIGINT | อันดับภายในวัน (1 = สตรีมมากที่สุด) |

---

### spotify_marts.monthly_genre_trends (Gold)

สรุปประสิทธิภาพแนวเพลงรายเดือน รวม MoM growth และ market share สำหรับการวิเคราะห์แนวโน้ม

| คอลัมน์ | ประเภท | คำอธิบาย |
|---|---|---|
| release_month | DATE | วันแรกของเดือน |
| release_year | INT | ปี |
| genre | TEXT | แนวเพลง |
| total_streams | NUMERIC | รวม stream ของแนวเพลงนี้ในเดือนนี้ |
| avg_popularity | NUMERIC(5,2) | ค่าเฉลี่ย popularity |
| avg_danceability | NUMERIC(5,3) | ค่าเฉลี่ย danceability |
| avg_energy | NUMERIC(5,3) | ค่าเฉลี่ย energy |
| avg_tempo | NUMERIC(7,2) | ค่าเฉลี่ย tempo (BPM) |
| unique_tracks | BIGINT | จำนวนเพลงที่แตกต่างกัน |
| unique_artists | BIGINT | จำนวนศิลปินที่แตกต่างกัน |
| market_reach | BIGINT | จำนวนประเทศที่มีการสตรีม |
| genre_share_pct | NUMERIC | สัดส่วน stream ต่อ stream รวมทั้งเดือน (รวมกันได้ 1.0 ต่อเดือน) |
| prev_month_streams | NUMERIC | ยอด stream เดือนก่อนหน้า (LAG) |
| mom_growth_pct | NUMERIC | อัตราการเติบโต month-over-month (%) |

---

## ตัวอย่าง Query

### ตรวจสอบ FK integrity ของ Star Schema

```sql
-- อัตราการ match ของ dim_artist
SELECT
    COUNT(*)                                          AS fact_rows,
    COUNT(da.artist_id)                               AS matched_artist,
    ROUND(COUNT(da.artist_id) * 100.0 / COUNT(*), 2) AS match_pct
FROM spotify.fact_streams fs
LEFT JOIN spotify.dim_artist da ON fs.artist_id = da.artist_id;

-- คาดหวัง: match_pct = 100.00
```

### Query ข้ามตาราง dim และ fact

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

### Top 5 ศิลปินรายวัน (Gold layer)

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

### แนวโน้ม MoM growth ของแนวเพลง (Gold layer)

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

### นับแถวแต่ละ layer เพื่อยืนยันผลลัพธ์

```sql
SELECT 'stg_spotify_tracks'  AS table_name, COUNT(*) AS rows FROM spotify.stg_spotify_tracks
UNION ALL
SELECT 'dim_artist',           COUNT(*) FROM spotify.dim_artist
UNION ALL
SELECT 'dim_album',            COUNT(*) FROM spotify.dim_album
UNION ALL
SELECT 'fact_streams',         COUNT(*) FROM spotify.fact_streams
UNION ALL
SELECT 'daily_top_artists',    COUNT(*) FROM spotify_marts.daily_top_artists
UNION ALL
SELECT 'monthly_genre_trends', COUNT(*) FROM spotify_marts.monthly_genre_trends;
```

รัน query ผ่าน terminal:
```bash
docker exec spotify_postgres psql -U airflow -d airflow -c "<query>"
```

---

## วิธีรัน

### ความต้องการเบื้องต้น

- Docker Desktop พร้อม WSL2 backend

### เริ่ม stack

```bash
docker-compose up -d --build
```

รอประมาณ 60 วินาทีให้ `airflow-init` ทำ DB migration และสร้าง admin user

### วางไฟล์ข้อมูล

```bash
cp /path/to/spotify_2015_2025_85k.csv data/raw_data/
```

### เปิด DAG

เข้า `http://localhost:8080` login ด้วย `admin / admin` ไปที่ `spotify_daily_etl_pipeline` แล้วกด Trigger

หรือใช้ CLI:
```bash
docker exec spotify_airflow_web airflow dags trigger spotify_daily_etl_pipeline
```

---

## โครงสร้างโปรเจกต์

```
ETL_SPOTIFY/
├── dags/
│   └── spotify_etl_dag.py              DAG หลัก — ทั้ง 6 task
├── dbt_spotify/
│   ├── dbt_project.yml
│   ├── profiles.yml                    การเชื่อมต่อ DB (สำหรับ local dev)
│   ├── packages.yml                    dependency: dbt_utils
│   ├── macros/
│   │   └── generate_schema_name.sql    override การตั้งชื่อ schema ของ dbt
│   └── models/
│       ├── staging/
│       │   ├── sources.yml             ประกาศ stg_spotify_tracks เป็น dbt source
│       │   └── stg_tracks_typed.sql    view ที่ cast type และเพิ่ม derived columns
│       ├── dimensions/
│       │   ├── dim_artist.sql
│       │   └── dim_album.sql
│       ├── facts/
│       │   └── fact_streams.sql
│       └── marts/
│           ├── daily_top_artists.sql
│           └── monthly_genre_trends.sql
├── data/
│   ├── raw_data/                       โซนรับไฟล์ — วางไฟล์ CSV ที่นี่
│   ├── cleansed/                       Bronze ชั่วคราว (ถูกลบหลัง run)
│   ├── rejected/                       DLQ — แถวที่ไม่ผ่านการตรวจสอบ
│   └── archive/                        ไฟล์ที่ประมวลผลแล้ว (ถาวร)
├── scripts/
│   └── entrypoint.sh                   Airflow init: db migrate + สร้าง admin user
├── Dockerfile                          ต่อยอดจาก airflow:2.8.1 ติดตั้ง dbt + providers
├── docker-compose.yml                  stack: Postgres + Airflow + pgAdmin
└── pgadmin_servers.json                การตั้งค่า pgAdmin server สำเร็จรูป
```

---

## ไฟล์ที่ commit และไม่ commit ขึ้น Git

| Path | Commit | เหตุผล |
|---|---|---|
| `dags/`, `dbt_spotify/models/`, `dbt_spotify/macros/` | ขึ้น | source code หลัก |
| `Dockerfile`, `docker-compose.yml` | ขึ้น | infrastructure as code |
| `dbt_spotify/dbt_project.yml`, `packages.yml`, `profiles.yml` | ขึ้น | config dbt (credential สำหรับ local dev เท่านั้น) |
| `pgadmin_servers.json` | ขึ้น | config server สำหรับ local dev |
| `data/raw_data/*.csv` | ไม่ขึ้น | ไฟล์ข้อมูลขนาดใหญ่ — แจกจ่ายแยกต่างหาก |
| `data/archive/*.csv`, `data/rejected/*.csv` | ไม่ขึ้น | output ที่ pipeline สร้าง |
| `dbt_spotify/dbt_packages/` | ไม่ขึ้น | ติดตั้งผ่าน `dbt deps` (เหมือน node_modules) |
| `dbt_spotify/target/` | ไม่ขึ้น | compiled artifacts ของ dbt |
| `.venv/` | ไม่ขึ้น | Python virtual environment |
| `.claude/` | ไม่ขึ้น | session files ของ editor |

หมายเหตุ: `profiles.yml` มี password แบบ plaintext สำหรับ PostgreSQL ใน local Docker environment ซึ่งยอมรับได้เนื่องจาก credential เดียวกันปรากฏใน `docker-compose.yml` อยู่แล้ว ห้ามใช้ production credential ในไฟล์นี้
