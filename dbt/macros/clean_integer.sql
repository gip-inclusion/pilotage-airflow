{% macro clean_integer(column_name) %}
    {#
        Convertit une colonne en integer :
        - trim du contenu
        - chaînes vides → null
        - cast direct en integer
    #}
    nullif(trim({{ adapter.quote(column_name) }}::text), '')::integer
{% endmacro %}
