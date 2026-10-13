-- Ookla speed tests per country, quarter and connection type.
-- The sum_* columns and tests are additive: average speed over any set of rows (a region, a
-- year, several quarters) is sum(sum_d_kbps_x_tests) / sum(tests). The avg_* columns are for
-- reading a single row only; never sum or average them.
select
    country_code,
    conn_type,
    quarter_start,
    year,
    quarter,
    tiles,
    tests,
    devices,
    sum_d_kbps_x_tests,
    sum_u_kbps_x_tests,
    sum_lat_ms_x_tests,
    round(sum_d_kbps_x_tests / tests / 1000, 2) as avg_download_mbps,
    round(sum_u_kbps_x_tests / tests / 1000, 2) as avg_upload_mbps,
    round(sum_lat_ms_x_tests / tests, 1) as avg_latency_ms,
    -- Too few tests for a stable average (e.g. Russia: ~800 fixed tests a quarter).
    tests < {{ var('min_country_tests', 1000) }} as is_low_sample
from {{ ref('stg_ookla_country_quarter') }}
