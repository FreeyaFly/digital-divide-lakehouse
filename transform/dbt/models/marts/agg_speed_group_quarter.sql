-- Test-weighted speed per world, region and income group, quarter and connection type, summed
-- from the additive country measures. "world" includes every unit with tests (also those
-- outside WDI, e.g. Taiwan); regions and income groups only contain WDI economies.
-- income_as_was uses the classification in force on the first day of the quarter.
with speed as (
    select * from {{ ref('fct_speed_country_quarter') }}
),

members as (
    select
        s.*,
        inline(array(
            named_struct('group_type', 'world', 'group_code', 'WLD', 'group_name', 'World'),
            named_struct('group_type', 'region', 'group_code', c.region_code,
                         'group_name', c.region_name),
            named_struct('group_type', 'income_current', 'group_code', c.income_code_current,
                         'group_name', c.income_name_current),
            named_struct('group_type', 'income_as_was', 'group_code', h.income_code,
                         'group_name', h.income_name)
        ))
    from speed as s
    inner join {{ ref('dim_country') }} as c
        on s.country_code = c.country_code
    left join {{ ref('dim_country_income_history') }} as h
        on s.country_code = h.country_code
        and c.in_wdi
        and s.quarter_start between h.valid_from and coalesce(h.valid_to, date'9999-12-31')
)

select
    group_type,
    group_code,
    any_value(group_name) as group_name,
    conn_type,
    quarter_start,
    count(*) as countries,
    sum(tests) as tests,
    sum(sum_d_kbps_x_tests) as sum_d_kbps_x_tests,
    sum(sum_u_kbps_x_tests) as sum_u_kbps_x_tests,
    sum(sum_lat_ms_x_tests) as sum_lat_ms_x_tests,
    round(sum(sum_d_kbps_x_tests) / sum(tests) / 1000, 2) as avg_download_mbps,
    round(sum(sum_u_kbps_x_tests) / sum(tests) / 1000, 2) as avg_upload_mbps,
    round(sum(sum_lat_ms_x_tests) / sum(tests), 1) as avg_latency_ms
from members
where group_code is not null
group by group_type, group_code, conn_type, quarter_start
