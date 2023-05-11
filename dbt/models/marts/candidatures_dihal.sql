select {{ dbt_utils.star(ref('candidatures_echelle_locale')) }}
from ref('candidatures_echelle_locale')
where 'type_org_prescripteur' in ('CHRS', 'CHU', 'RS_FJT', 'OIL')
