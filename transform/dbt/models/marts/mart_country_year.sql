-- One row per WDI economy and year, indicators side by side with speed: the table behind the
-- cross-country charts (e.g. GDP per capita vs. fixed download speed). Speed per year pools the
-- quarters available in that calendar year (quarters_* says how many: Ookla data here runs from
-- 2024 Q4 to 2026 Q3, so only 2025 is a full year).
with indicators as (
    select
        country_code,
        year,
        {% for code, name in [
            ('IT.NET.USER.ZS', 'internet_users_pct'),
            ('IT.NET.BBND.P2', 'fixed_broadband_per_100'),
            ('IT.CEL.SETS.P2', 'mobile_subscriptions_per_100'),
            ('IT.NET.SECR.P6', 'secure_servers_per_million'),
            ('EG.ELC.ACCS.ZS', 'electricity_access_pct'),
            ('SP.URB.TOTL.IN.ZS', 'urban_population_pct'),
            ('NY.GDP.PCAP.PP.CD', 'gdp_per_capita_ppp'),
            ('NY.GDP.PCAP.CD', 'gdp_per_capita_usd'),
            ('SP.POP.TOTL', 'population'),
            ('SE.TER.ENRR', 'tertiary_enrolment_gross_pct')
        ] %}
        max(case when indicator_code = '{{ code }}' then value end) as {{ name }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('fct_indicator_country_year') }}
    group by country_code, year
),

speed as (
    select
        country_code,
        year,
        {% for conn in ['fixed', 'mobile'] %}
        count_if(conn_type = '{{ conn }}') as quarters_{{ conn }},
        sum(case when conn_type = '{{ conn }}' then tests end) as tests_{{ conn }},
        round(
            sum(case when conn_type = '{{ conn }}' then sum_d_kbps_x_tests end)
            / sum(case when conn_type = '{{ conn }}' then tests end) / 1000, 2
        ) as {{ conn }}_download_mbps
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('fct_speed_country_quarter') }}
    group by country_code, year
)

select
    c.country_code,
    c.country_name,
    c.region_name,
    c.income_name_current,
    coalesce(i.year, s.year) as year,
    i.internet_users_pct,
    i.fixed_broadband_per_100,
    i.mobile_subscriptions_per_100,
    i.secure_servers_per_million,
    i.electricity_access_pct,
    i.urban_population_pct,
    i.gdp_per_capita_ppp,
    i.gdp_per_capita_usd,
    i.population,
    i.tertiary_enrolment_gross_pct,
    s.quarters_fixed,
    s.tests_fixed,
    s.fixed_download_mbps,
    s.quarters_mobile,
    s.tests_mobile,
    s.mobile_download_mbps
from indicators as i
full outer join speed as s
    on i.country_code = s.country_code and i.year = s.year
inner join {{ ref('dim_country') }} as c
    on coalesce(i.country_code, s.country_code) = c.country_code
where c.in_wdi
