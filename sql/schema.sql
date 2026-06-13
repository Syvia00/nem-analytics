-- NEM spot prices table, range-partitioned by month for 2026.
-- Parent table holds no data; each child covers one calendar month.

CREATE TABLE IF NOT EXISTS spot_prices (
    id              BIGSERIAL,
    ts              TIMESTAMPTZ          NOT NULL,   -- settlement interval timestamp (AEST/UTC+10)
    region          VARCHAR(10)          NOT NULL,   -- NEM region: NSW1, QLD1, SA1, TAS1, VIC1
    price           NUMERIC(10, 2)       NOT NULL,   -- dispatch price ($/MWh)
    demand          NUMERIC(12, 3)       NOT NULL,   -- regional demand (MW)
    available_gen   NUMERIC(12, 3),                  -- available generation (MW)
    PRIMARY KEY (id, ts),
    UNIQUE (ts, region)
) PARTITION BY RANGE (ts);

-- ── 2026 monthly partitions ───────────────────────────────────────────────────

CREATE TABLE spot_prices_2026_01 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE spot_prices_2026_02 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE spot_prices_2026_03 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE spot_prices_2026_04 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE spot_prices_2026_05 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE spot_prices_2026_06 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE spot_prices_2026_07 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE spot_prices_2026_08 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE spot_prices_2026_09 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

CREATE TABLE spot_prices_2026_10 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

CREATE TABLE spot_prices_2026_11 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

CREATE TABLE spot_prices_2026_12 PARTITION OF spot_prices
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- ── Indexes (created on each partition automatically) ─────────────────────────

CREATE INDEX ON spot_prices (ts);
CREATE INDEX ON spot_prices (region, ts);

-- ── Analytics results ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS price_analytics (
    ts               TIMESTAMPTZ      NOT NULL,
    region           VARCHAR(10)      NOT NULL,
    price            NUMERIC(10, 4),
    demand           NUMERIC(12, 4),
    rolling_7d_avg   NUMERIC(10, 4),   -- 7-day rolling mean price
    rolling_7d_vol   NUMERIC(10, 4),   -- 7-day rolling std  price
    is_peak          BOOLEAN,          -- true = 07:00–21:59 AEST
    peak_period_avg  NUMERIC(10, 4),   -- avg price for region/month/peak-period group
    anomaly_score    NUMERIC(10, 6),   -- IsolationForest decision_function (lower = more anomalous)
    is_anomaly       BOOLEAN,          -- true = flagged by IsolationForest (contamination 0.05)
    PRIMARY KEY (ts, region)
);

-- ── Forecast results ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS price_forecast (
    ds          TIMESTAMPTZ     NOT NULL,   -- forecast interval (AEST)
    region      VARCHAR(10)     NOT NULL,
    yhat        NUMERIC(10, 4),             -- point forecast ($/MWh)
    yhat_lower  NUMERIC(10, 4),             -- 95% lower bound
    yhat_upper  NUMERIC(10, 4),             -- 95% upper bound
    run_ts      TIMESTAMPTZ     NOT NULL,   -- when the model was run
    PRIMARY KEY (ds, region)
);

-- ── Migrations ────────────────────────────────────────────────────────────────

ALTER TABLE spot_prices ADD COLUMN IF NOT EXISTS available_gen NUMERIC(12, 3);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'spot_prices_ts_region_uniq'
    ) THEN
        ALTER TABLE spot_prices ADD CONSTRAINT spot_prices_ts_region_uniq UNIQUE (ts, region);
    END IF;
END $$;