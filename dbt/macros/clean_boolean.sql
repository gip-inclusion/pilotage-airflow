{% macro clean_boolean(column_name) %}
    {#
        Convertit une colonne en boolean :
        - trim + lower
        - 'oui' / 'true' → true
        - 'non' / 'false' → false
        - valeurs vides ou non reconnues → null
    #}
    case
        when nullif(trim({{ adapter.quote(column_name) }}::text), '') is null then null
        when lower(trim({{ adapter.quote(column_name) }}::text)) in ('oui', 'true') then true
        when lower(trim({{ adapter.quote(column_name) }}::text)) in ('non', 'false') then false
        else null
    end
{% endmacro %}
