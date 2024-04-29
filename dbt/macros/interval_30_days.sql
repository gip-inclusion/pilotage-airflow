{% macro interval_30_days(date_diff, order) %}
    case
        when {{ date_diff }} between 0 and 30 and {{ order }} = 0 then '[0-30]'
        when {{ date_diff }} between 0 and 30 and {{ order }} = 1 then '1'
        when {{ date_diff }} between 31 and 60 and {{ order }} = 0 then '[31-60]'
        when {{ date_diff }} between 31 and 60 and {{ order }} = 1 then '2'
        when {{ date_diff }} between 61 and 90 and {{ order }} = 0 then '[61-90]'
        when {{ date_diff }} between 61 and 90 and {{ order }} = 1 then '3'
        when {{ date_diff }} between 91 and 120 and {{ order }} = 0 then '[91-120]'
        when {{ date_diff }} between 91 and 120 and {{ order }} = 1 then '4'
        when {{ date_diff }} between 121 and 150 and {{ order }} = 0 then '[121-150]'
        when {{ date_diff }} between 121 and 150 and {{ order }} = 1 then '5'
        when {{ date_diff }} between 151 and 180 and {{ order }} = 0 then '[151-180]'
        when {{ date_diff }} between 151 and 180 and {{ order }} = 1 then '6'
        when {{ date_diff }} between 181 and 210 and {{ order }} = 0 then '[181-210]'
        when {{ date_diff }} between 181 and 210 and {{ order }} = 1 then '7'
        when {{ date_diff }} > 211 and {{ order }} = 0 then 'more than 210'
        when {{ date_diff }} > 211 and {{ order }} = 1 then '8'
    end
{% endmacro %}
