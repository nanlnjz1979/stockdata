ATTACH TABLE _ UUID '14c91fb9-35a7-410f-8664-058ae59ec576'
(
    `ingest_date` DateTime,
    `code` String,
    `name` String,
    `buy_amount` Float64,
    `buy_times` Int32,
    `sell_amount` Float64,
    `sell_times` Int32,
    `net_amount` Float64,
    `query_type` Int32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ingest_date)
ORDER BY (code, query_type, ingest_date)
SETTINGS index_granularity = 8192
