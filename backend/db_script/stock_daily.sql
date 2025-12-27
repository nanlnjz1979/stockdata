ATTACH TABLE _ UUID 'a2f840a9-6f6c-412e-bac5-cb546365ff99'
(
    `code` String,
    `date` DateTime,
    `open` Float64 CODEC(Delta(8), ZSTD(1)),
    `close` Float64 CODEC(Delta(8), ZSTD(1)),
    `high` Float64 CODEC(Delta(8), ZSTD(1)),
    `low` Float64 CODEC(Delta(8), ZSTD(1)),
    `volume` Int64 CODEC(Delta(8), ZSTD(1)),
    `amount` Float64 CODEC(ZSTD(1)),
    `turnover` Float64 CODEC(ZSTD(1)),
    `outstanding_share` Float64 CODEC(ZSTD(1)),
    `_version` UInt64 DEFAULT toUnixTimestamp64Nano(now64()) + rowNumberInAllBlocks()
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(date)
PRIMARY KEY (code, date)
ORDER BY (code, date)
TTL date + toIntervalYear(10)
SETTINGS index_granularity = 8192
