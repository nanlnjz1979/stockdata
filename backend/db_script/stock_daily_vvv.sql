-- Latest stock price, adjustment factor, industry classification, and finance data.
DROP VIEW IF EXISTS default.stock_daily_vvv;

CREATE VIEW default.stock_daily_vvv AS
WITH
    latest_daily AS
    (
        SELECT
            code,
            argMax(close, tuple(date, _version)) AS latest_close
        FROM default.stock_daily
        GROUP BY code
    ),
    latest_factor AS
    (
        SELECT
            code,
            argMax(hfq, tuple(date, _version)) AS hfq
        FROM default.fq_factor
        GROUP BY code
    )
SELECT
    industry.code AS code,
    industry.market AS market,
    industry.stock_name AS name,
    industry.sw_first_level AS sw_first_level,
    industry.sw_second_level AS sw_second_level,
    industry.sw_third_level AS sw_third_level,
    latest_daily.latest_close AS close,
    latest_factor.hfq AS hfq,
    CAST(NULL AS Nullable(Float64)) AS pe,
    CAST(NULL AS Nullable(Float64)) AS pe_ttm,
    CAST(NULL AS Nullable(Float64)) AS pb,
    CAST(NULL AS Nullable(Float64)) AS dividend_yield,
    CAST(NULL AS Nullable(Float64)) AS market_cap,
    latest_fin.* EXCEPT (code, date)
FROM default.sw_industry_stocks_v AS industry
LEFT JOIN latest_daily ON industry.code = latest_daily.code
LEFT JOIN latest_factor ON industry.code = latest_factor.code
LEFT JOIN default.stock_fin_v1 AS latest_fin ON industry.code = latest_fin.code
SETTINGS join_use_nulls = 1;
