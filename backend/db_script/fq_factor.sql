CREATE TABLE default.fq_factor
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
TTL date + toIntervalYear(50)
SETTINGS index_granularity = 8192