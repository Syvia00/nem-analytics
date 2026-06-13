import csv
import io
import logging
import os
import zipfile
from datetime import datetime
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

# NEM time is AEST (UTC+10), no daylight saving
_NEM_TZ = ZoneInfo("Australia/Brisbane")

INSERT_SQL = """
    INSERT INTO spot_prices (ts, region, price, demand, available_gen)
    VALUES %s
    ON CONFLICT (ts, region) DO NOTHING
"""


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
        day_folders = sorted(d for d in RAW_DATA_PATH.iterdir() if d.is_dir())
        log.info("Found %d day folder(s) under %s", len(day_folders), RAW_DATA_PATH)

        for day_folder in day_folders:
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
