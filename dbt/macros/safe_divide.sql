{% macro safe_divide(numerator, denominator) %}
    round(coalesce({{ numerator }}::numeric / nullif({{ denominator }}, 0), 0), 3)
{% endmacro %}
