ATTACH VIEW _ UUID '003767e1-4065-4f39-95c0-1de2830372ce'
(
    `code` String,
    `date` DateTime,
    `qfq` Float64,
    `hfq` Float64
)
AS SELECT
    code,
    date,
    argMax(qfq, _version) AS qfq,
    argMax(hfq, _version) AS hfq
FROM default.fq_factor
GROUP BY
    code,
    date
