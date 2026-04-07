CREATE TABLE default.stock_index
(
    `code` String,
    `name` String,
    `index_code` String,
    `index_name` String,
    `_version` UInt64 DEFAULT toUnixTimestamp64Nano(now64()) + rowNumberInAllBlocks()
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY index_name
PRIMARY KEY (code, index_code)
ORDER BY (code, index_code)
SETTINGS index_granularity = 8192
