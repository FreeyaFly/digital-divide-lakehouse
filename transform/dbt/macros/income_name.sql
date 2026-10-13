{# World Bank income group name for a WDI income code. #}
{% macro income_name(code) -%}
    case {{ code }}
        when 'LIC' then 'Low income'
        when 'LMC' then 'Lower middle income'
        when 'UMC' then 'Upper middle income'
        when 'HIC' then 'High income'
    end
{%- endmacro %}
