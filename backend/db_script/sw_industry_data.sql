CREATE TABLE default.sw_industry_data
(
    `industry_code` String,
    `industry_name` String,
    `parent_industry` String,
    `component_count` Int32,
    `static_pe` Float64,
    `ttm_pe` Float64,
    `pb_ratio` Float64,
    `static_dividend_yield` Float64,
    `timestamp` DateTime,
    `_version` UInt64 DEFAULT toUnixTimestamp64Nano(now64()) + rowNumberInAllBlocks()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (industry_code, timestamp)
TTL timestamp + toIntervalYear(10)
SETTINGS index_granularity = 8192