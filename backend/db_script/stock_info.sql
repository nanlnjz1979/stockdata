CREATE TABLE default.stock_info
(
    `code` String,
    `name` String,
    `company_name` String,
    `market` String,
    `listing_date` Date,
    `_version` UInt64 DEFAULT toUnixTimestamp64Nano(now64()),
    `_updated` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY substring(market, 1, 2)
PRIMARY KEY (code, market)
ORDER BY (code, market)
TTL _updated + toIntervalMonth(36)
SETTINGS index_granularity = 8192
