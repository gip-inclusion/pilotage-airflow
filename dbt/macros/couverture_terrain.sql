{% macro couverture(besoin, offre) %}
    round(
        coalesce(
            {{ besoin }}::numeric / nullif({{ besoin }} + {{ offre }}, 0),
            0
        ),
        5
    )
{% endmacro %}
