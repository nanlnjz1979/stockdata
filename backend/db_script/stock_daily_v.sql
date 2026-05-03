CREATE VIEW default.stock_daily_v
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
    `outstanding_share` Float64
)
AS SELECT
    code,
    date,
    argMax(open, _version) AS open,
    argMax(close, _version) AS close,
    argMax(high, _version) AS high,
    argMax(low, _version) AS low,
    argMax(volume, _version) AS volume,
    argMax(amount, _version) AS amount,
    argMax(turnover, _version) AS turnover,
    argMax(outstanding_share, _version) AS outstanding_share
FROM default.stock_daily
GROUP BY
    code,
    date
