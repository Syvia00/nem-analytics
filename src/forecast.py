import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from prophet import Prophet
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]

REGION = "NSW1"
HOLDOUT_DAYS = 7
FORECAST_DAYS = 7
FREQ = "5min"
_PERIODS_PER_DAY = 288        # 24 × 12 five-minute slots
_AEST = ZoneInfo("Australia/Brisbane")

_LOAD_SQL = """
SELECT (ts AT TIME ZONE 'Australia/Brisbane')::timestamp AS ds,
       price::double precision AS y
FROM spot_prices
WHERE region = %s
ORDER BY ts;
"""

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS price_forecast (
    ds          TIMESTAMPTZ     NOT NULL,
    region      VARCHAR(10)     NOT NULL,
    yhat        NUMERIC(10, 4),
    yhat_lower  NUMERIC(10, 4),
    yhat_upper  NUMERIC(10, 4),
    run_ts      TIMESTAMPTZ     NOT NULL,
    PRIMARY KEY (ds, region)
);
"""

_UPSERT_SQL = """
INSERT INTO price_forecast (ds, region, yhat, yhat_lower, yhat_upper, run_ts)
VALUES %s
ON CONFLICT (ds, region) DO UPDATE SET
    yhat       = EXCLUDED.yhat,
    yhat_lower = EXCLUDED.yhat_lower,
    yhat_upper = EXCLUDED.yhat_upper,
    run_ts     = EXCLUDED.run_ts;
"""


# ── Data loading ──────────────────────────────────────────────────────────────

def load_data(conn) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(_LOAD_SQL, (REGION,))
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["ds", "y"])
    df["ds"] = pd.to_datetime(df["ds"])   # timezone-naive AEST, as Prophet expects
    df["y"] = pd.to_numeric(df["y"])
    log.info("Loaded %d intervals for %s  (%s → %s)",
             len(df), REGION, df["ds"].min().date(), df["ds"].max().date())
    return df


# ── Model factory ─────────────────────────────────────────────────────────────

def _make_model() -> Prophet:
    return Prophet(
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=False,   # need a full year of data for this to be meaningful
        interval_width=0.95,
        seasonality_mode="multiplicative",  # electricity prices scale multiplicatively
    )


# ── MAE on holdout ────────────────────────────────────────────────────────────

def evaluate_mae(df: pd.DataFrame) -> float:
    """
    Fit on all-but-last-7-days, forecast over the holdout window,
    return MAE in $/MWh.
    """
    min_rows = 2 * HOLDOUT_DAYS * _PERIODS_PER_DAY
    if len(df) < min_rows:
        log.warning(
            "Only %d rows — need at least %d for holdout MAE; skipping.",
            len(df), min_rows,
        )
        return float("nan")

    cutoff = df["ds"].max() - pd.Timedelta(days=HOLDOUT_DAYS)
    train = df[df["ds"] <= cutoff].copy()
    holdout = df[df["ds"] > cutoff].copy()

    log.info("Holdout fit: %d training rows, %d holdout rows (cutoff %s)",
             len(train), len(holdout), cutoff.date())

    m = _make_model()
    m.fit(train)

    future = m.make_future_dataframe(periods=len(holdout), freq=FREQ, include_history=False)
    forecast = m.predict(future)

    merged = holdout.merge(forecast[["ds", "yhat"]], on="ds", how="inner")
    if merged.empty:
        log.warning("No timestamp overlap between holdout and forecast — check data gaps.")
        return float("nan")

    mae = float(np.mean(np.abs(merged["y"] - merged["yhat"])))
    log.info(
        "Holdout MAE  : %.4f $/MWh  over %d intervals (%d days)",
        mae, len(merged), HOLDOUT_DAYS,
    )
    return mae


# ── Final forecast ────────────────────────────────────────────────────────────

def generate_forecast(df: pd.DataFrame, run_ts: datetime) -> pd.DataFrame:
    """Fit on full history, project FORECAST_DAYS ahead."""
    log.info("Fitting final model on all %d rows…", len(df))
    m = _make_model()
    m.fit(df)

    periods = FORECAST_DAYS * _PERIODS_PER_DAY
    future = m.make_future_dataframe(periods=periods, freq=FREQ, include_history=False)
    forecast = m.predict(future)

    out = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    out["region"] = REGION
    out["run_ts"] = run_ts
    log.info("Forecast window: %s → %s  (%d rows)",
             out["ds"].min().date(), out["ds"].max().date(), len(out))
    return out


# ── Persistence ───────────────────────────────────────────────────────────────

def save_forecast(conn, forecast: pd.DataFrame) -> None:
    records = [
        (
            row.ds.to_pydatetime().replace(tzinfo=_AEST),
            row.region,
            round(float(row.yhat), 4),
            round(float(row.yhat_lower), 4),
            round(float(row.yhat_upper), 4),
            row.run_ts,
        )
        for row in forecast.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
        execute_values(cur, _UPSERT_SQL, records, page_size=500)
    conn.commit()
    log.info("Upserted %d rows into price_forecast", len(records))


# ── Entry point ───────────────────────────────────────────────────────────────

def run() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    run_ts = datetime.now(timezone.utc)

    try:
        df = load_data(conn)
        if df.empty:
            log.error("No data found for %s — run ingest.py first.", REGION)
            return

        mae = evaluate_mae(df)
        if not np.isnan(mae):
            log.info("MAE = %.4f $/MWh", mae)

        forecast = generate_forecast(df, run_ts)
        save_forecast(conn, forecast)
    finally:
        conn.close()

    log.info("Done.")


if __name__ == "__main__":
    run()
