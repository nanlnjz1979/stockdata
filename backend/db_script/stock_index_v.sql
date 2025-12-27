ATTACH VIEW _ UUID '1414d877-623c-41b7-a74f-496461be8bb2'
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
