select
    {% if env_var('CI', '') %}
        id
    {% else %}
        {{ dbt_utils.star(ref('stg_reseaux')) }}
    {% endif %}
from
    {{ ref('stg_reseaux') }}
