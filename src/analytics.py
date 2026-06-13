import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]

# Peak hours in AEST: 07:00 inclusive to 22:00 exclusive
_PEAK_START = 7
_PEAK_END = 22

_LOAD_SQL = """
SELECT ts, region, price, demand
FROM spot_prices
ORDER BY region, ts;
"""

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS price_analytics (
    ts               TIMESTAMPTZ      NOT NULL,
    region           VARCHAR(10)      NOT NULL,
    price            NUMERIC(10, 4),
    demand           NUMERIC(12, 4),
    rolling_7d_avg   NUMERIC(10, 4),
    rolling_7d_vol   NUMERIC(10, 4),
    is_peak          BOOLEAN,
    peak_period_avg  NUMERIC(10, 4),
    anomaly_score    NUMERIC(10, 6),
    is_anomaly       BOOLEAN,
    PRIMARY KEY (ts, region)
);
"""

_UPSERT_SQL = """
INSERT INTO price_analytics
    (ts, region, price, demand,
     rolling_7d_avg, rolling_7d_vol,
     is_peak, peak_period_avg,
     anomaly_score, is_anomaly)
VALUES %s
ON CONFLICT (ts, region) DO UPDATE SET
    price           = EXCLUDED.price,
    demand          = EXCLUDED.demand,
    rolling_7d_avg  = EXCLUDED.rolling_7d_avg,
    rolling_7d_vol  = EXCLUDED.rolling_7d_vol,
    is_peak         = EXCLUDED.is_peak,
    peak_period_avg = EXCLUDED.peak_period_avg,
    anomaly_score   = EXCLUDED.anomaly_score,
    is_anomaly      = EXCLUDED.is_anomaly;
"""


# ── Step 1: rolling 7-day avg and volatility ──────────────────────────────────

def add_rolling_metrics(df: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for _, grp in df.groupby("region", sort=False):
        grp = grp.sort_values("ts").set_index("ts")
        grp["rolling_7d_avg"] = grp["price"].rolling("7D", min_periods=1).mean()
        # min_periods=2 so single-point windows return NaN instead of 0
        grp["rolling_7d_vol"] = grp["price"].rolling("7D", min_periods=2).std()
        parts.append(grp.reset_index())
    return pd.concat(parts, ignore_index=True)


# ── Step 2: peak / off-peak average by region and month ───────────────────────

def add_peak_offpeak(df: pd.DataFrame) -> pd.DataFrame:
    ts_aest = df["ts"].dt.tz_convert("Australia/Brisbane")
    df["is_peak"] = (ts_aest.dt.hour >= _PEAK_START) & (ts_aest.dt.hour < _PEAK_END)
    # Period label used only for grouping; dropped after join
    df["_month"] = ts_aest.dt.to_period("M").astype(str)
    df["peak_period_avg"] = df.groupby(["region", "_month", "is_peak"])["price"].transform("mean")
    df.drop(columns="_month", inplace=True)
    return df


# ── Step 3: anomaly detection via Isolation Forest ────────────────────────────

def add_anomaly_flags(df: pd.DataFrame) -> pd.DataFrame:
    feature_cols = ["price", "demand"]
    valid = df[feature_cols].notna().all(axis=1)

    df["anomaly_score"] = np.nan
    df["is_anomaly"] = pd.NA

    X_raw = df.loc[valid, feature_cols].values.astype(float)
    X = StandardScaler().fit_transform(X_raw)

    iso = IsolationForest(contamination=0.05, random_state=42, n_jobs=-1)
    iso.fit(X)

    df.loc[valid, "anomaly_score"] = iso.decision_function(X)
    df.loc[valid, "is_anomaly"] = iso.predict(X) == -1

    n_flagged = int(df["is_anomaly"].sum())
    log.info("Isolation Forest: %d / %d rows flagged as anomalies (%.1f%%)",
             n_flagged, valid.sum(), 100 * n_flagged / valid.sum())
    return df


# ── Orchestration ─────────────────────────────────────────────────────────────

def _load(conn) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(_LOAD_SQL)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    # Ensure ts is timezone-aware (psycopg2 returns tz-aware datetimes)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["price"] = pd.to_numeric(df["price"])
    df["demand"] = pd.to_numeric(df["demand"])
    log.info("Loaded %d rows from spot_prices", len(df))
    return df


def _save(conn, df: pd.DataFrame) -> None:
    def _scalar(v):
        if pd.isna(v):
            return None
        if isinstance(v, (np.bool_,)):
            return bool(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        return v

    records = [
        (
            row.ts.to_pydatetime(),
            row.region,
            _scalar(row.price),
            _scalar(row.demand),
            _scalar(row.rolling_7d_avg),
            _scalar(row.rolling_7d_vol),
            _scalar(row.is_peak),
            _scalar(row.peak_period_avg),
            _scalar(row.anomaly_score),
            _scalar(row.is_anomaly),
        )
        for row in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
        execute_values(cur, _UPSERT_SQL, records, page_size=2000)
        log.info("Upserted %d rows into price_analytics", len(records))
    conn.commit()


def run_analytics() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        df = _load(conn)
        if df.empty:
            log.warning("spot_prices is empty — run ingest.py first.")
            return

        log.info("Computing rolling 7-day metrics…")
        df = add_rolling_metrics(df)

        log.info("Computing peak / off-peak averages…")
        df = add_peak_offpeak(df)

        log.info("Running Isolation Forest anomaly detection…")
        df = add_anomaly_flags(df)

        _save(conn, df)
    finally:
        conn.close()

    log.info("Done.")


if __name__ == "__main__":
    run_analytics()
