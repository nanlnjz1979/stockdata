ATTACH TABLE _ UUID '43cd9d42-8adf-417d-af50-35868596e0e3'
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
PARTITION BY toDate(ingest_date)
ORDER BY (code, ingest_date, query_type)
SETTINGS index_granularity = 8192
