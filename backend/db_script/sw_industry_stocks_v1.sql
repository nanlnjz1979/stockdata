CREATE VIEW default.sw_industry_stocks_v1
(
    `code` String,
    `market` String,
    `stock_name` String,
    `sw_first_level` String,
    `sw_second_level` String,
    `sw_third_level` String,
    `price` Float64,
    `pe` Float64,
    `pe_ttm` Float64,
    `pb` Float64,
    `dividend_yield` Float64,
    `market_cap` Float64
)
AS SELECT
    argMax(a.stock_name, a._version) AS stock_name,
    argMax(b.parent_industry, a._version) AS sw_second_level,
    argMax(c.parent_industry, a._version) AS sw_first_level,
    argMax(a.sw_third_level, a._version) AS sw_third_level,
    argMax(a.price, a._version) AS price,
    argMax(a.pe, a._version) AS pe,
    argMax(a.pe_ttm, a._version) AS pe_ttm,
    argMax(a.pb, a._version) AS pb,
    argMax(a.dividend_yield, a._version) AS dividend_yield,
    argMax(a.market_cap, a._version) AS market_cap,
    substring(a.stock_code, 1, 6) AS code,
    substring(a.stock_code, 8, 9) AS market
FROM default.sw_industry_stocks AS a
LEFT JOIN default.sw_industry_data_v AS b ON a.industry_code = b.industry_code
LEFT JOIN default.sw_industry_data_v AS c ON b.parent_industry = c.industry_name
GROUP BY a.stock_code