ATTACH VIEW _ UUID '37c556cb-f448-4cb6-8d50-09faa240f5d4'
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
    s.open * f.qfq AS open,
    s.close * f.qfq AS close,
    s.high * f.qfq AS high,
    s.low * f.qfq AS low,
    s.volume,
    s.amount,
    s.turnover,
    s.outstanding_share
FROM default.stock_daily AS s
INNER JOIN
(
    SELECT
        code,
        max(date) AS latest_fq_date
    FROM default.fq_factor
    GROUP BY code
) AS latest ON s.code = latest.code
INNER JOIN
(
    SELECT
        code,
        date,
        argMax(qfq, _version) AS qfq
    FROM default.fq_factor
    GROUP BY
        code,
        date
) AS f ON (f.code = latest.code) AND (f.date = latest.latest_fq_date)
WHERE s.date >= latest.latest_fq_date
