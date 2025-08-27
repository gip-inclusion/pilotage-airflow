select distinct
    {{ pilo_star(ref('candidats'), relation_alias='cdd') }},
    candidatures_odc.dept_org_prescripteur,
    candidatures_odc.nom_dept_org_prescripteur,
    candidatures_odc.region_org_prescripteur,
    candidatures_odc.nom_org_prescripteur
from {{ ref('candidats') }} as cdd
inner join
    {{ ref('candidatures_odc') }} as candidatures_odc
    on cdd.id = candidatures_odc.id_candidat
