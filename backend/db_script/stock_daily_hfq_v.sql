ATTACH VIEW _ UUID '87ec571e-327f-41d7-b94b-37de910b22ba'
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
    s.code AS code,
    s.date AS date,
    s.open * f.hfq AS open,
    s.close * f.hfq AS close,
    s.high * f.hfq AS high,
    s.low * f.hfq AS low,
    s.volume,
    s.amount,
    s.turnover,
    s.outstanding_share
FROM default.stock_daily AS s
INNER JOIN
(
    SELECT
        s.code,
        s.date,
        max(f.date) AS latest_fq_date
    FROM default.stock_daily AS s
    INNER JOIN default.fq_factor AS f ON (s.code = f.code) AND (s.date >= f.date)
    GROUP BY
        s.code,
        s.date
) AS latest ON (s.code = latest.code) AND (s.date = latest.date)
INNER JOIN
(
    SELECT
        code,
        date,
        argMax(hfq, _version) AS hfq
    FROM default.fq_factor
    GROUP BY
        code,
        date
) AS f ON (f.code = latest.code) AND (f.date = latest.latest_fq_date)
