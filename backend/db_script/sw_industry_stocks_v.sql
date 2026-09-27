DROP VIEW IF EXISTS default.sw_industry_stocks_v;

CREATE VIEW default.sw_industry_stocks_v
(
    `code` String,
    `market` String,
    `stock_name` String,
    `sw_first_level` String,
    `sw_second_level` String,
    `sw_third_level` String,
    `update_time` DateTime
)
AS SELECT
    code,
    toString(argMax(market, _version)) AS market,
    argMax(stock_name, _version) AS stock_name,
    toString(argMax(sw_first_level, _version)) AS sw_first_level,
    toString(argMax(sw_second_level, _version)) AS sw_second_level,
    toString(argMax(sw_third_level, _version)) AS sw_third_level,
    max(update_time) AS update_time
FROM default.sw_industry_stocks
GROUP BY code;
