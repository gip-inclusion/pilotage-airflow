-- dbt_macros.sql
{% macro interval_30_days(date_diff) %}
    case
        when {{ date_diff }} between 0 and 30 then '[0-30]'
        when {{ date_diff }} between 31 and 60 then '[30-60]'
        when {{ date_diff }} between 61 and 90 then '[60-90]'
        when {{ date_diff }} between 91 and 120 then '[90-120]'
        when {{ date_diff }} between 121 and 150 then '[120-150]'
        when {{ date_diff }} between 151 and 180 then '[150-180]'
        when {{ date_diff }} between 181 and 210 then '[180-210]'
        else 'more than 210'
    end
{% endmacro %}
