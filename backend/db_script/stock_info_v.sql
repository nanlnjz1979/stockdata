ATTACH VIEW _ UUID '086a3e23-faee-4286-8ffe-4493d2010b9b'
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
