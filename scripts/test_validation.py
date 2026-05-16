"""
Test script: ทดสอบ validation logic จาก Task 1
รันตรงบน Windows ไม่ต้องผ่าน Docker หรือ Airflow

Usage:
    python scripts/test_validation.py
"""
import csv
import io
from datetime import datetime
from pathlib import Path

# ── Mock data: ออกแบบให้มีทั้ง valid และ invalid แบบต่างๆ ─────────────────────
MOCK_ROWS = [
    # แถวที่ควรผ่านทั้งหมด
    {
        "track_id": "TRACK001",
        "track_name": "Good Song",
        "artist_name": "Good Artist",
        "album_name": "Good Album",
        "release_date": "2023-01-15",
        "genre": "Pop",
        "duration_ms": "210000",
        "popularity": "75",
        "danceability": "0.750",
        "energy": "0.800",
        "key": "5",
        "loudness": "-5.20",
        "mode": "1",
        "instrumentalness": "0.0010",
        "tempo": "120.00",
        "stream_count": "1500000",
        "country": "TH",
        "explicit": "0",
        "label": "Sony Music",
        "expect": "VALID",
    },

    # 1. track_id ว่างเปล่า
    {
        "track_id": "",
        "track_name": "No ID Song",
        "artist_name": "Artist A",
        "album_name": "Album A",
        "release_date": "2022-05-10",
        "genre": "Rock",
        "duration_ms": "180000",
        "popularity": "60",
        "danceability": "0.600",
        "energy": "0.700",
        "key": "3",
        "loudness": "-8.00",
        "mode": "0",
        "instrumentalness": "0.0000",
        "tempo": "95.00",
        "stream_count": "500000",
        "country": "US",
        "explicit": "1",
        "label": "Universal",
        "expect": "REJECT — track_id is null or empty",
    },

    # 2. release_date เป็นอนาคต
    {
        "track_id": "TRACK003",
        "track_name": "Future Song",
        "artist_name": "Artist B",
        "album_name": "Album B",
        "release_date": "2099-12-31",
        "genre": "Jazz",
        "duration_ms": "240000",
        "popularity": "50",
        "danceability": "0.400",
        "energy": "0.300",
        "key": "7",
        "loudness": "-10.00",
        "mode": "1",
        "instrumentalness": "0.5000",
        "tempo": "85.00",
        "stream_count": "0",
        "country": "JP",
        "explicit": "false",
        "label": "Warner",
        "expect": "REJECT — release_date is in the future",
    },

    # 3. duration_ms เป็น 0
    {
        "track_id": "TRACK004",
        "track_name": "Zero Duration",
        "artist_name": "Artist C",
        "album_name": "Album C",
        "release_date": "2021-03-20",
        "genre": "Hip-Hop",
        "duration_ms": "0",
        "popularity": "80",
        "danceability": "0.900",
        "energy": "0.850",
        "key": "1",
        "loudness": "-3.50",
        "mode": "0",
        "instrumentalness": "0.0000",
        "tempo": "140.00",
        "stream_count": "2000000",
        "country": "US",
        "explicit": "1",
        "label": "Def Jam",
        "expect": "REJECT — duration_ms must be > 0",
    },

    # 4. popularity เกิน 100
    {
        "track_id": "TRACK005",
        "track_name": "Too Popular",
        "artist_name": "Artist D",
        "album_name": "Album D",
        "release_date": "2020-07-04",
        "genre": "EDM",
        "duration_ms": "195000",
        "popularity": "150",
        "danceability": "0.950",
        "energy": "0.980",
        "key": "0",
        "loudness": "-2.00",
        "mode": "1",
        "instrumentalness": "0.0010",
        "tempo": "128.00",
        "stream_count": "9000000",
        "country": "UK",
        "explicit": "false",
        "label": "Spinnin",
        "expect": "REJECT — popularity out of range",
    },

    # 5. danceability > 1.0
    {
        "track_id": "TRACK006",
        "track_name": "Too Danceable",
        "artist_name": "Artist E",
        "album_name": "Album E",
        "release_date": "2019-11-11",
        "genre": "Dance",
        "duration_ms": "220000",
        "popularity": "70",
        "danceability": "1.500",
        "energy": "0.750",
        "key": "4",
        "loudness": "-6.00",
        "mode": "1",
        "instrumentalness": "0.0000",
        "tempo": "125.00",
        "stream_count": "750000",
        "country": "DE",
        "explicit": "0",
        "label": "Ministry",
        "expect": "REJECT — danceability out of range",
    },

    # 6. stream_count ติดลบ
    {
        "track_id": "TRACK007",
        "track_name": "Negative Streams",
        "artist_name": "Artist F",
        "album_name": "Album F",
        "release_date": "2018-06-15",
        "genre": "Classical",
        "duration_ms": "360000",
        "popularity": "30",
        "danceability": "0.200",
        "energy": "0.150",
        "key": "9",
        "loudness": "-20.00",
        "mode": "1",
        "instrumentalness": "0.9500",
        "tempo": "72.00",
        "stream_count": "-100",
        "country": "FR",
        "explicit": "false",
        "label": "Deutsche Grammophon",
        "expect": "REJECT — stream_count must be >= 0",
    },

    # 7. explicit ค่าผิด
    {
        "track_id": "TRACK008",
        "track_name": "Bad Explicit Flag",
        "artist_name": "Artist G",
        "album_name": "Album G",
        "release_date": "2022-09-01",
        "genre": "R&B",
        "duration_ms": "200000",
        "popularity": "65",
        "danceability": "0.700",
        "energy": "0.600",
        "key": "2",
        "loudness": "-7.50",
        "mode": "0",
        "instrumentalness": "0.0020",
        "tempo": "90.00",
        "stream_count": "300000",
        "country": "US",
        "explicit": "yes",
        "label": "RCA",
        "expect": "REJECT — explicit must be 0 or 1",
    },

    # 8. หลายข้อผิดพร้อมกัน (track_name ว่าง + loudness เกินช่วง)
    {
        "track_id": "TRACK009",
        "track_name": "",
        "artist_name": "Artist H",
        "album_name": "Album H",
        "release_date": "2023-04-01",
        "genre": "Metal",
        "duration_ms": "300000",
        "popularity": "55",
        "danceability": "0.300",
        "energy": "0.950",
        "key": "6",
        "loudness": "5.00",
        "mode": "1",
        "instrumentalness": "0.0100",
        "tempo": "160.00",
        "stream_count": "100000",
        "country": "NO",
        "explicit": "1",
        "label": "Nuclear Blast",
        "expect": "REJECT — track_name empty + loudness > 0",
    },
]

FIELDNAMES = [
    "track_id", "track_name", "artist_name", "album_name", "release_date",
    "genre", "duration_ms", "popularity", "danceability", "energy", "key",
    "loudness", "mode", "instrumentalness", "tempo", "stream_count",
    "country", "explicit", "label",
]


# ── Validation logic (คัดลอกมาจาก spotify_etl_dag.py Task 1) ─────────────────
def validate_row(row: dict) -> list[str]:
    errors = []

    def _float(field, default=-999):
        try:
            return float(row.get(field, default))
        except (ValueError, TypeError):
            return None

    if not row.get("track_id", "").strip():
        errors.append("track_id is null or empty")
    if not row.get("track_name", "").strip():
        errors.append("track_name is null or empty")
    if not row.get("artist_name", "").strip():
        errors.append("artist_name is null or empty")

    raw_date = row.get("release_date", "").strip()
    DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"]
    parsed_date = None
    for fmt in DATE_FORMATS:
        try:
            parsed_date = datetime.strptime(raw_date, fmt).date()
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

    return errors


# ── Run test ──────────────────────────────────────────────────────────────────
def run_test():
    valid_rows, rejected_rows = [], []
    ingestion_ts = datetime.utcnow().isoformat()

    SEP = "-" * 70

    print(f"\n{'=' * 70}")
    print(f"  VALIDATION TEST — {len(MOCK_ROWS)} mock rows")
    print(f"{'=' * 70}\n")

    for i, row in enumerate(MOCK_ROWS, 1):
        data = {k: row[k] for k in FIELDNAMES}
        expected = row["expect"]
        errors = validate_row(data)

        if errors:
            data["error_reason"] = "; ".join(errors)
            data["ingestion_timestamp"] = ingestion_ts
            rejected_rows.append(data)
            status = "REJECTED"
        else:
            valid_rows.append(data)
            status = "VALID   "

        track = data["track_id"] or "(empty)"
        print(f"  Row {i:02d} [{status}]  track_id={track:<10}  expect: {expected}")
        if errors:
            for e in errors:
                print(f"           -> {e}")

    print(f"\n{SEP}")
    print(f"  SUMMARY")
    print(f"{SEP}")
    print(f"  Total rows   : {len(MOCK_ROWS)}")
    print(f"  Valid        : {len(valid_rows)}")
    print(f"  Rejected     : {len(rejected_rows)}")
    print(f"{SEP}\n")

    # ── เขียนผลออกไฟล์จริง ───────────────────────────────────────────────────
    out_dir = Path(__file__).parent.parent / "data"
    cleansed_path = out_dir / "cleansed" / "test_cleansed.csv"
    rejected_path = out_dir / "rejected" / "test_rejected.csv"

    cleansed_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)

    with open(cleansed_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(valid_rows)

    rejected_fields = FIELDNAMES + ["error_reason", "ingestion_timestamp"]
    with open(rejected_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rejected_fields)
        writer.writeheader()
        writer.writerows(rejected_rows)

    print(f"  Output files:")
    print(f"  Valid    → {cleansed_path}")
    print(f"  Rejected → {rejected_path}\n")


if __name__ == "__main__":
    run_test()
