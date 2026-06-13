import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

DATABASE_URL = os.environ["DATABASE_URL"]
REPORT_PATH = Path(__file__).parent.parent / "validation_report.txt"

# ── Queries ───────────────────────────────────────────────────────────────────

_NULL_PRICES = """
SELECT region, COUNT(*) AS null_count
FROM spot_prices
WHERE price IS NULL
GROUP BY region
ORDER BY region;
"""

# Cast ts to AEST date so day boundaries align with NEM trading day.
_MISSING_INTERVALS = """
SELECT day, region, count,
       288 - count AS missing
FROM (
    SELECT (ts AT TIME ZONE 'Australia/Brisbane')::date AS day,
           region,
           COUNT(*) AS count
    FROM spot_prices
    GROUP BY 1, 2
) daily
WHERE count <> 288
ORDER BY day, region;
"""

_PRICE_SPIKES = """
WITH stats AS (
    SELECT region,
           AVG(price)    AS mean,
           STDDEV(price) AS std
    FROM spot_prices
    GROUP BY region
),
scored AS (
    SELECT sp.ts,
           sp.region,
           sp.price,
           ROUND(
               (sp.price - st.mean) / NULLIF(st.std, 0),
           2) AS z_score
    FROM spot_prices sp
    JOIN stats st USING (region)
)
SELECT region, ts, price, z_score
FROM scored
WHERE ABS(z_score) > 3
ORDER BY ABS(z_score) DESC;
"""

_NEGATIVE_PRICES = """
SELECT region,
       COUNT(*)              AS occurrences,
       ROUND(MIN(price), 2)  AS min_price,
       ROUND(MAX(price), 2)  AS max_price,
       ROUND(AVG(price), 2)  AS avg_price
FROM spot_prices
WHERE price < 0
GROUP BY region
ORDER BY min_price;
"""

_TOTAL_ROWS = "SELECT COUNT(*) FROM spot_prices;"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run(cur, sql: str) -> list[dict]:
    cur.execute(sql)
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _section(lines: list[str], title: str) -> None:
    lines.append(f"\n{'─' * 60}")
    lines.append(f"  {title}")
    lines.append(f"{'─' * 60}")


def _table(lines: list[str], rows: list[dict]) -> None:
    if not rows:
        lines.append("  (none)")
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}
    header = "  " + "  ".join(c.upper().ljust(widths[c]) for c in cols)
    lines.append(header)
    lines.append("  " + "  ".join("-" * widths[c] for c in cols))
    for row in rows:
        lines.append("  " + "  ".join(str(row[c]).ljust(widths[c]) for c in cols))


# ── Checks ────────────────────────────────────────────────────────────────────

def check_null_prices(cur) -> tuple[list[str], int]:
    rows = _run(cur, _NULL_PRICES)
    total = sum(r["null_count"] for r in rows)
    lines: list[str] = []
    _section(lines, "[1] NULL PRICES")
    if total == 0:
        lines.append("  OK — no null prices found.")
    else:
        lines.append(f"  FAIL — {total} null price(s) across {len(rows)} region(s).")
        _table(lines, rows)
    return lines, total


def check_missing_intervals(cur) -> tuple[list[str], int]:
    rows = _run(cur, _MISSING_INTERVALS)
    lines: list[str] = []
    _section(lines, "[2] MISSING 5-MINUTE INTERVALS  (expected 288 rows/region/day)")
    if not rows:
        lines.append("  OK — all days have exactly 288 intervals per region.")
    else:
        total_missing = sum(r["missing"] for r in rows)
        lines.append(f"  WARN — {len(rows)} day-region pair(s) with incomplete intervals "
                     f"({total_missing} missing slot(s) total).")
        _table(lines, rows)
    return lines, len(rows)


def check_price_spikes(cur) -> tuple[list[str], int]:
    rows = _run(cur, _PRICE_SPIKES)
    lines: list[str] = []
    _section(lines, "[3] PRICE SPIKES  (|z-score| > 3, computed per region)")
    if not rows:
        lines.append("  OK — no price spikes detected.")
    else:
        lines.append(f"  WARN — {len(rows)} interval(s) with |z-score| > 3.")
        _table(lines, rows[:50])
        if len(rows) > 50:
            lines.append(f"  ... {len(rows) - 50} more row(s) omitted from report.")
    return lines, len(rows)


def check_negative_prices(cur) -> tuple[list[str], int]:
    rows = _run(cur, _NEGATIVE_PRICES)
    lines: list[str] = []
    _section(lines, "[4] NEGATIVE PRICES  (valid in NEM but flagged for review)")
    if not rows:
        lines.append("  INFO — no negative prices in this dataset.")
    else:
        total = sum(r["occurrences"] for r in rows)
        lines.append(f"  INFO — {total} negative price interval(s) across "
                     f"{len(rows)} region(s).")
        _table(lines, rows)
    return lines, total


# ── Main ──────────────────────────────────────────────────────────────────────

def validate() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    report: list[str] = []

    try:
        with conn.cursor() as cur:
            cur.execute(_TOTAL_ROWS)
            total_rows = cur.fetchone()[0]

            report.append("=" * 60)
            report.append("  NEM Analytics — Spot Price Validation Report")
            report.append(f"  Generated : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            report.append(f"  Table rows: {total_rows:,}")
            report.append("=" * 60)

            if total_rows == 0:
                report.append("\n  WARNING: spot_prices table is empty — run ingest.py first.")
                _write_report(report)
                return

            for check_fn in (
                check_null_prices,
                check_missing_intervals,
                check_price_spikes,
                check_negative_prices,
            ):
                lines, _ = check_fn(cur)
                report.extend(lines)

    finally:
        conn.close()

    report.append(f"\n{'─' * 60}")
    report.append("  End of report")
    report.append(f"{'─' * 60}\n")

    _write_report(report)


def _write_report(lines: list[str]) -> None:
    text = "\n".join(lines)
    REPORT_PATH.write_text(text, encoding="utf-8")
    print(text)
    print(f"\nReport written to {REPORT_PATH}")


if __name__ == "__main__":
    validate()
