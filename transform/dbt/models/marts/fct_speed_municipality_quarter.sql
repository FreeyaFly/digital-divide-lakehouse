-- Ookla speed tests per Dutch municipality, quarter and connection type, as additive sums.
-- Mobile samples are thin (about half the municipalities have fewer than 100 tests a quarter),
-- so each row also carries the sums of the last four quarters, ending with this one: pooling
-- additive sums gives a stable test-weighted average without averaging averages.
with base as (
    select * from {{ ref('stg_ookla_nl_municipality_quarter') }}
),

pooled as (
    select
        *,
        count(*) over w4 as quarters_4q,
        sum(tests) over w4 as tests_4q,
        sum(sum_d_kbps_x_tests) over w4 as sum_d_kbps_x_tests_4q,
        sum(sum_u_kbps_x_tests) over w4 as sum_u_kbps_x_tests_4q
    from base
    window w4 as (
        partition by municipality_code, conn_type
        order by quarter_start
        rows between 3 preceding and current row
    )
)

select
    municipality_code,
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
    tests < {{ var('min_municipality_tests', 100) }} as is_low_sample,
    quarters_4q,
    tests_4q,
    sum_d_kbps_x_tests_4q,
    sum_u_kbps_x_tests_4q,
    round(sum_d_kbps_x_tests_4q / tests_4q / 1000, 2) as avg_download_mbps_4q,
    round(sum_u_kbps_x_tests_4q / tests_4q / 1000, 2) as avg_upload_mbps_4q
from pooled
