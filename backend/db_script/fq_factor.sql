ATTACH TABLE _ UUID '9c343dfb-d95a-45a4-b67b-e84796907cae'
(
    `code` String,
    `date` DateTime,
    `qfq` Float64,
    `hfq` Float64,
    `_version` UInt64 DEFAULT toUnixTimestamp64Nano(now64()) + rowNumberInAllBlocks()
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY code
PRIMARY KEY (code, date)
ORDER BY (code, date)
TTL date + toIntervalYear(10)
SETTINGS index_granularity = 8192
