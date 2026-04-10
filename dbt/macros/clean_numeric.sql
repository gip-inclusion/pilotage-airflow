{% macro clean_numeric(column_name) %}
    {#
        Convertit une colonne en numeric :
        - trim du contenu
        - chaînes vides → null
        - cast direct en numeric (supporte les décimales)
    #}
    nullif(trim({{ adapter.quote(column_name) }}::text), '')::numeric
{% endmacro %}
