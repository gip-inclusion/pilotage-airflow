{% macro completeness_case(column_name) %}
    case
        when {{ column_name }} is null then 0
        when trim(cast({{ column_name }} as {{ dbt.type_string() }})) = '' then 0
        else 1
    end
{% endmacro %}
