{% macro date_id_from_date(date_expression) -%}
    case
        when ({{ date_expression }}) is null then -1
        when ({{ date_expression }}) between date '{{ var("dim_date_start_date") }}'
            and date '{{ var("dim_date_end_date") }}'
            then extract(year from ({{ date_expression }}))::integer * 10000
                + extract(month from ({{ date_expression }}))::integer * 100
                + extract(day from ({{ date_expression }}))::integer
    end
{%- endmacro %}
