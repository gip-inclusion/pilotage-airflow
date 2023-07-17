select {{ pilo_star(ref('candidatures_echelle_locale')) }}
from {{ ref('candidatures_echelle_locale') }}
where id_org_prescripteur in (select id from {{ ref('stg_prescripteurs_odc') }})
