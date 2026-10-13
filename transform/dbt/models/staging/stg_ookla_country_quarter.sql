select
    country_code,
    conn_type,
    period_start as quarter_start,
    year,
    quarter,
    tiles,
    tests,
    devices,
    sum_d_kbps_x_tests,
    sum_u_kbps_x_tests,
    sum_lat_ms_x_tests
from {{ source('silver', 'silver_ookla_country_quarter') }}
