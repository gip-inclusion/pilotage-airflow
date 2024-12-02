{% macro sum_columns(columns) %}
    {% for column in columns %}
        coalesce({{ column }}, 0)
        {% if not loop.last %} + {% endif %}
    {% endfor %}
{% endmacro %}
