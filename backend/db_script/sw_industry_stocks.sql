ATTACH TABLE _ UUID '3f14cf1e-44ab-4b8d-8950-94a05cbcde87'
(
    `stock_code` String,
    `stock_name` String,
    `industry_code` String,
    `include_date` Date,
    `sw_first_level` String,
    `sw_second_level` String,
    `sw_third_level` String,
    `price` Float64,
    `pe` Float64,
    `pe_ttm` Float64,
    `pb` Float64,
    `dividend_yield` Float64,
    `market_cap` Float64,
    `net_profit_growth_0930` Float64,
    `net_profit_growth_0630` Float64,
    `revenue_growth_0930` Float64,
    `revenue_growth_0630` Float64,
    `update_time` DateTime,
    `_version` UInt64 DEFAULT toUnixTimestamp64Nano(now64()) + rowNumberInAllBlocks()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(update_time)
ORDER BY (stock_code, update_time)
TTL update_time + toIntervalYear(10)
SETTINGS index_granularity = 8192
