select
    c.id_structure,
    c.nom_org_prescripteur,
    sum(c.nombre_de_candidatures) as somme_candidatures_ph
from
    {{ ref('eph_nbr_candidatures') }} as c
where
    c.origine = 'Prescripteur habilité'
group by
    c.id_structure,
    c.nom_org_prescripteur
