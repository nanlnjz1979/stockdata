ATTACH VIEW _ UUID 'f647dc2c-4d5c-41f2-9721-66e2a4497e1b'
(
    `stock_code` String,
    `code` String,
    `market` String,
    `stock_name` String,
    `industry_code` String,
    `include_date` Date,
    `sw_first_level` String,
    `sw_second_level` String,
    `sw_third_level` String,
    `price` Float64,
    `pe` Float64,
    `pe_ttm` Float64,
    `pb` Float64,
    `dividend_yield` Float64,
    `market_cap` Float64,
    `net_profit_growth_0930` Float64,
    `net_profit_growth_0630` Float64,
    `revenue_growth_0930` Float64,
    `revenue_growth_0630` Float64,
    `update_time` DateTime
)
AS SELECT
    a.stock_code,
    argMax(a.stock_name, a._version) AS stock_name,
    argMax(a.industry_code, a._version) AS industry_code,
    argMax(a.include_date, a._version) AS include_date,
    argMax(b.parent_industry, a._version) AS sw_second_level,
    argMax(c.parent_industry, a._version) AS sw_first_level,
    argMax(a.sw_third_level, a._version) AS sw_third_level,
    argMax(a.price, a._version) AS price,
    argMax(a.pe, a._version) AS pe,
    argMax(a.pe_ttm, a._version) AS pe_ttm,
    argMax(a.pb, a._version) AS pb,
    argMax(a.dividend_yield, a._version) AS dividend_yield,
    argMax(a.market_cap, a._version) AS market_cap,
    argMax(a.net_profit_growth_0930, a._version) AS net_profit_growth_0930,
    argMax(a.net_profit_growth_0630, a._version) AS net_profit_growth_0630,
    argMax(a.revenue_growth_0930, a._version) AS revenue_growth_0930,
    argMax(a.revenue_growth_0630, a._version) AS revenue_growth_0630,
    argMax(a.update_time, a._version) AS update_time,
    substring(a.stock_code, 1, 6) AS code,
    substring(a.stock_code, 8, 9) AS market
FROM default.sw_industry_stocks AS a
LEFT JOIN default.sw_industry_data_v AS b ON a.industry_code = b.industry_code
LEFT JOIN default.sw_industry_data_v AS c ON b.parent_industry = c.industry_name
GROUP BY a.stock_code
