ATTACH TABLE _ UUID '4fef3166-80cd-45da-beec-885f9ccc3fe8'
(
    `code` String,
    `date` DateTime,
    `open` Float64,
    `close` Float64,
    `high` Float64,
    `low` Float64,
    `volume` Int64,
    `amount` Float64,
    `turnover` Float64,
    `outstanding_share` Float64,
    `_version` UInt64
)
ENGINE = Distributed('QW_stock', 'default', 'stock_daily', cityHash64(code))
