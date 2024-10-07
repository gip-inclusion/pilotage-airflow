select
    {{ pilo_star(ref('candidatures_echelle_locale'), relation_alias='cel') }}
from {{ ref('candidatures_echelle_locale') }} as cel
where cel.type_org_prescripteur in ('CHRS', 'CHU', 'RS_FJT', 'OIL')
