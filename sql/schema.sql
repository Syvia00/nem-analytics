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
