-- The 14 WDI indicators, with a short name for reports and the rule for group aggregates:
--   population_weighted  shares, per-100 and per-capita values: sum(value × population) / sum(population)
--   sum                  counts (population)
--   not_aggregated       the right weight is not available (female/male, rural/urban or
--                        tertiary-age population), so no group value is computed
with indicators as (
    select indicator_code, any_value(indicator_name) as indicator_name, any_value(unit) as unit
    from {{ ref('stg_wb_indicator_values') }}
    group by indicator_code
)

select
    indicator_code,
    indicator_name,
    unit,
    case indicator_code
        when 'IT.NET.USER.ZS' then 'internet_users_pct'
        when 'IT.NET.USER.FE.ZS' then 'internet_users_female_pct'
        when 'IT.NET.USER.MA.ZS' then 'internet_users_male_pct'
        when 'IT.NET.BBND.P2' then 'fixed_broadband_per_100'
        when 'IT.CEL.SETS.P2' then 'mobile_subscriptions_per_100'
        when 'IT.NET.SECR.P6' then 'secure_servers_per_million'
        when 'EG.ELC.ACCS.ZS' then 'electricity_access_pct'
        when 'EG.ELC.ACCS.RU.ZS' then 'electricity_access_rural_pct'
        when 'EG.ELC.ACCS.UR.ZS' then 'electricity_access_urban_pct'
        when 'SP.URB.TOTL.IN.ZS' then 'urban_population_pct'
        when 'NY.GDP.PCAP.CD' then 'gdp_per_capita_usd'
        when 'NY.GDP.PCAP.PP.CD' then 'gdp_per_capita_ppp'
        when 'SP.POP.TOTL' then 'population'
        when 'SE.TER.ENRR' then 'tertiary_enrolment_gross_pct'
    end as short_name,
    case
        when indicator_code like 'IT.%' then 'ICT'
        when indicator_code like 'EG.%' then 'Energy'
        when indicator_code like 'NY.%' then 'Economy'
        when indicator_code like 'SP.%' then 'Population'
        when indicator_code like 'SE.%' then 'Education'
    end as topic,
    case
        when indicator_code = 'SP.POP.TOTL' then 'sum'
        when indicator_code in ('IT.NET.USER.FE.ZS', 'IT.NET.USER.MA.ZS', 'EG.ELC.ACCS.RU.ZS',
                                'EG.ELC.ACCS.UR.ZS', 'SE.TER.ENRR') then 'not_aggregated'
        else 'population_weighted'
    end as group_aggregation
from indicators
