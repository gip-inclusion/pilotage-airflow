{#
  Macro: months_after_exit

  Extracts the number of months from a text label such as
  "3 mois", "6 mois", "12 mois" and returns it as an integer.

  Centralizes this parsing logic to ensure consistency across all
  staging models that transform DSN data.
#}
{% macro months_after_exit(col) %}
    regexp_replace({{ col }}, '[^0-9]', '', 'g')::int
{% endmacro %}
