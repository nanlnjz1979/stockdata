CREATE VIEW default.stock_index_v
(
    `code` String,
    `name` String,
    `index_code` String,
    `index_name` String
)
AS SELECT
    code,
    argMax(name, _version) AS name,
    index_code,
    argMax(index_name, _version) AS index_name
FROM default.stock_index
GROUP BY
    code,
    index_code