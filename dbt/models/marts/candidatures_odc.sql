select {{ pilo_star(ref('candidatures_echelle_locale'), relation_alias="cel") }}
from {{ ref('candidatures_echelle_locale') }} as cel
inner join
    {{ ref('stg_prescripteurs_odc') }} as prescripteurs_odc
    on prescripteurs_odc.id = cel.id_org_prescripteur
