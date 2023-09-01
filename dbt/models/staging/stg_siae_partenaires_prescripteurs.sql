select
    organisations.id,
    count(distinct cel.id_structure) as nb_siae
from
    {{ ref('stg_organisations') }} as organisations
left join {{ ref('candidatures_echelle_locale') }} as cel
    on organisations.id = cel.id_org_prescripteur
group by organisations.id
