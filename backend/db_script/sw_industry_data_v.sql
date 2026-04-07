CREATE VIEW default.sw_industry_data_v
(
    `industry_code` String,
    `industry_name` String,
    `parent_industry` String,
    `component_count` Int32,
    `static_pe` Float64,
    `ttm_pe` Float64,
    `pb_ratio` Float64,
    `static_dividend_yield` Float64,
    `timestamp` DateTime
)
AS SELECT
    industry_code,
    argMax(industry_name, _version) AS industry_name,
    argMax(parent_industry, _version) AS parent_industry,
    argMax(component_count, _version) AS component_count,
    argMax(static_pe, _version) AS static_pe,
    argMax(ttm_pe, _version) AS ttm_pe,
    argMax(pb_ratio, _version) AS pb_ratio,
    argMax(static_dividend_yield, _version) AS static_dividend_yield,
    argMax(timestamp, _version) AS timestamp
FROM default.sw_industry_data
GROUP BY industry_code