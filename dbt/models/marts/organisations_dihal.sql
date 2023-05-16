select
    {% if env_var('CI', '') %}
        id
    {% else %}
        {{ dbt_utils.star(ref('stg_organisations')) }}
    {% endif %}
from {{ ref('stg_organisations') }}
where type in ('CHRS', 'CHU', 'RS_FJT', 'OIL')
