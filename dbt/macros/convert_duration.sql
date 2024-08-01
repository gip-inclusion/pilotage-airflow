{% macro duration_in_days(start, end) %}
    -- we use epoch to compute seconds between start and end date; and convert to days be dividing by 86400
    cast(extract(epoch from cast({{ end }} as timestamp) - cast({{ start }} as timestamp)) / 86400 as int)
{% endmacro %}

{% macro duration_in_months(start, end) %}
    -- we use epoch to compute seconds between start and end date; and convert to months be dividing by 2629746
    round(extract(epoch from cast({{ end }} as timestamp) - cast({{ start }} as timestamp)) / 2629746, 2)
{% endmacro %}
