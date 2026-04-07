CREATE VIEW default.fq_factor_v
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
