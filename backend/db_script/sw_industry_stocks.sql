CREATE TABLE IF NOT EXISTS default.sw_industry_stocks
(
    `code` String,
    `market` LowCardinality(String),
    `stock_name` String,
    `sw_first_level` LowCardinality(String),
    `sw_second_level` LowCardinality(String),
    `sw_third_level` LowCardinality(String),
    `update_time` DateTime DEFAULT now(),
    `_version` UInt64 DEFAULT toUnixTimestamp64Nano(now64()) + rowNumberInAllBlocks()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY code
SETTINGS index_granularity = 8192;
