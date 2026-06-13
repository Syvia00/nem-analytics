import csv
import io
import logging
import os
import zipfile
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
RAW_DATA_PATH = Path(os.getenv("RAW_DATA_PATH", Path(__file__).parent.parent / "raw_data"))
DATABASE_URL = os.environ["DATABASE_URL"]

# Date range for backfill — adjust and re-run to load a new month without
# touching data that is already fully loaded.
START_DATE = date(2026, 5, 1)
END_DATE   = date(2026, 5, 31)

# A day is "fully loaded" once every NEM region has this many rows for it.
_NEM_REGIONS     = {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}
_INTERVALS_PER_DAY = 288

# NEM time is AEST (UTC+10), no daylight saving
_NEM_TZ = ZoneInfo("Australia/Brisbane")

INSERT_SQL = """
    INSERT INTO spot_prices (ts, region, price, demand, available_gen)
    VALUES %s
    ON CONFLICT (ts, region) DO NOTHING
"""


def _folder_date(folder: Path) -> date | None:
    """Extract the calendar date from a PUBLIC_P5MIN_YYYYMMDD folder name."""
    parts = folder.name.rsplit("_", 1)
    if len(parts) == 2 and len(parts[1]) == 8:
        try:
            return datetime.strptime(parts[1], "%Y%m%d").date()
        except ValueError:
            pass
    return None


_FULLY_LOADED_SQL = """
SELECT day
FROM (
    SELECT (ts AT TIME ZONE 'Australia/Brisbane')::date AS day,
           region,
           COUNT(*) AS cnt
    FROM spot_prices
    GROUP BY 1, 2
) region_counts
GROUP BY day
HAVING COUNT(DISTINCT region) = %(n_regions)s
   AND MIN(cnt) >= %(intervals)s;
"""


def _fully_loaded_dates(conn) -> set[date]:
    """Return dates where every NEM region already has a full day of rows."""
    with conn.cursor() as cur:
        cur.execute(_FULLY_LOADED_SQL, {
            "n_regions": len(_NEM_REGIONS),
            "intervals": _INTERVALS_PER_DAY,
        })
        return {row[0] for row in cur.fetchall()}


def _parse_nem_dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y/%m/%d %H:%M:%S").replace(tzinfo=_NEM_TZ)


def _open_csv_text(path: Path) -> str:
    """Return raw text from a .zip (single CSV inside) or a .csv file."""
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            with zf.open(zf.namelist()[0]) as f:
                return f.read().decode("utf-8", errors="replace")
    return path.read_text(encoding="utf-8", errors="replace")


def parse_file(path: Path) -> list[tuple]:
    """
    Return (ts, region, price, demand, available_gen) tuples from one P5MIN file.
    Keeps only REGIONSOLUTION D rows where INTERVENTION == 0.
    """
    text = _open_csv_text(path)
    col_map: dict[str, int] = {}
    rows: list[tuple] = []

    for record in csv.reader(io.StringIO(text)):
        if len(record) < 3:
            continue
        row_type, table = record[0], record[2]
        if table != "REGIONSOLUTION":
            continue
        if row_type == "I":
            col_map = {name: i for i, name in enumerate(record)}
        elif row_type == "D" and col_map:
            if record[col_map["INTERVENTION"]] != "0":
                continue
            rows.append((
                _parse_nem_dt(record[col_map["INTERVAL_DATETIME"]]),
                record[col_map["REGIONID"]],
                record[col_map["RRP"]],
                record[col_map["TOTALDEMAND"]],
                record[col_map["AVAILABLEGENERATION"]] or None,
            ))
    return rows


def ingest() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    total_inserted = 0

    try:
        loaded = _fully_loaded_dates(conn)
        log.info(
            "Date range : %s → %s", START_DATE, END_DATE,
        )
        log.info("Already fully loaded: %d date(s) — will skip", len(loaded))

        all_folders = sorted(d for d in RAW_DATA_PATH.iterdir() if d.is_dir())

        # Filter to folders whose date falls within the configured range
        # and haven't been fully loaded yet.
        pending = []
        for folder in all_folders:
            d = _folder_date(folder)
            if d is None:
                continue
            if d < START_DATE or d > END_DATE:
                continue
            if d in loaded:
                log.debug("Skipping %s — already fully loaded", folder.name)
                continue
            pending.append((d, folder))

        log.info("%d day folder(s) to process", len(pending))

        for _, day_folder in pending:
            data_files = sorted(
                p for p in day_folder.iterdir()
                if p.suffix.lower() in {".zip", ".csv"}
            )
            if not data_files:
                continue

            log.info("Processing %s (%d file(s))", day_folder.name, len(data_files))
            day_rows: list[tuple] = []

            for path in data_files:
                try:
                    day_rows.extend(parse_file(path))
                except Exception as exc:
                    log.warning("Skipping %s: %s", path.name, exc)

            if not day_rows:
                log.info("  No REGIONSOLUTION rows found, skipping")
                continue

            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, day_rows)
                inserted = cur.rowcount

            conn.commit()
            log.info("  Inserted %d / %d row(s)", inserted, len(day_rows))
            total_inserted += inserted

    finally:
        conn.close()

    log.info("Done. Total rows inserted: %d", total_inserted)


if __name__ == "__main__":
    ingest()
