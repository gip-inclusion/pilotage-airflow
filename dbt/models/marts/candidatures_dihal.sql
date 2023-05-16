select
    {% if env_var('CI', '') %}
        id
    {% else %}
        {{ dbt_utils.star(ref('candidatures_echelle_locale')) }}
    {% endif %}
from {{ ref('candidatures_echelle_locale') }}
where "type_org_prescripteur" in ('CHRS', 'CHU', 'RS_FJT', 'OIL')
