-- Dutch tiles summed per municipality, quarter and connection type, as additive measures.
select
    gemeente_code as municipality_code,
    conn_type,
    period_start as quarter_start,
    year,
    quarter,
    count(*) as tiles,
    sum(tests) as tests,
    sum(devices) as devices,
    sum(avg_d_kbps * tests) as sum_d_kbps_x_tests,
    sum(avg_u_kbps * tests) as sum_u_kbps_x_tests,
    sum(avg_lat_ms * tests) as sum_lat_ms_x_tests
from {{ source('silver', 'silver_ookla_nl_tiles') }}
group by all
