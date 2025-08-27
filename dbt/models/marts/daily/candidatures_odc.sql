select
    {{ pilo_star(ref('candidatures_echelle_locale'), relation_alias="cel") }},
    prescripteurs_odc."région"          as region_org_prescripteur,
    prescripteurs_odc."nom_département" as nom_dept_org_prescripteur,
    prescripteurs_odc."département"     as dept_org_prescripteur
from {{ ref('candidatures_echelle_locale') }} as cel
inner join
    {{ ref('stg_prescripteurs_odc') }} as prescripteurs_odc
    on cel.id_org_prescripteur = prescripteurs_odc.id
