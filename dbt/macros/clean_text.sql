{% macro clean_text(column_name) %}
    {#
        Nettoie une colonne texte :
        - cast en text
        - supprime les espaces en début et fin (trim)
        - convertit les chaînes vides en null
    #}
    nullif(trim({{ adapter.quote(column_name) }}::text), '')
{% endmacro %}
