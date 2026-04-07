CREATE VIEW default.stock_info_v
(
    `code` String,
    `name` String,
    `company_name` String,
    `market` String,
    `listing_date` Date
)
AS SELECT
    code,
    market,
    argMax(name, _version) AS name,
    argMax(company_name, _version) AS company_name,
    argMax(listing_date, _version) AS listing_date
FROM default.stock_info
GROUP BY
    code,
    market
