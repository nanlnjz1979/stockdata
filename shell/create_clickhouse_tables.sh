#!/bin/bash

# 创建ClickHouse表的脚本
# 集群名字：QW_stock

cat << EOF | clickhouse-client -n
--单机--
CREATE TABLE default.stock_daily
(
    code String,
    date DateTime,

    open Float64 CODEC(Delta, ZSTD),
    close Float64 CODEC(Delta, ZSTD),
    high Float64 CODEC(Delta, ZSTD),
    low Float64 CODEC(Delta, ZSTD),

    volume Int64 CODEC(Delta, ZSTD),
    amount Float64 CODEC(ZSTD),
    turnover Float64 CODEC(ZSTD),
    outstanding_share Float64 CODEC(ZSTD),

    _version UInt64
        DEFAULT toUnixTimestamp64Nano(now64()) + rowNumberInAllBlocks()
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(date)
ORDER BY (code, date)
PRIMARY KEY (code, date)
TTL date + INTERVAL 10 YEAR;

--集群--
CREATE TABLE default.stock_daily 
( 
code String, 
date DateTime, 


open Float64 CODEC(Delta, ZSTD), 
close Float64 CODEC(Delta, ZSTD), 
high Float64 CODEC(Delta, ZSTD), 
low Float64 CODEC(Delta, ZSTD), 


volume Int64 CODEC(Delta, ZSTD), 
amount Float64 CODEC(ZSTD), 
turnover Float64 CODEC(ZSTD), 
outstanding_share Float64 CODEC(ZSTD), 


_version UInt64 
DEFAULT toUnixTimestamp64Nano(now64()) + rowNumberInAllBlocks() 
) 
ENGINE = ReplicatedReplacingMergeTree( 
'/clickhouse/tables/shard1/stock_daily', 
'replica1', 
_version 
) 
PARTITION BY toYYYYMM(date) 
PRIMARY KEY (code, date) 
ORDER BY (code, date) 
TTL date + INTERVAL 10 YEAR 
SETTINGS 
index_granularity = 8192, 
min_rows_for_wide_part = 10000000;


-- 分布式表，作为应用入口 
CREATE TABLE default.stock_daily_all 
( 
code String, 
date DateTime, 
open Float64, 
close Float64, 
high Float64, 
low Float64, 
volume Int64, 
amount Float64, 
turnover Float64, 
outstanding_share Float64, 
_version UInt64 
) 
ENGINE = Distributed( 
QW_stock, -- 集群名字，单机可随意填 
default, -- 本地数据库名 
stock_daily, -- 本地表名 
cityHash64(code) -- 分片 key 
);
EOF

echo "ClickHouse表创建脚本执行完成！"