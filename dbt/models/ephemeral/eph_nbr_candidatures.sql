select
    c."état",
    c.date_candidature,
    c.id_structure,
    c.type_structure,
    c.nom_structure,
    c.injection_ai,
    c.origine,
    c."origine_détaillée",
    c."département_structure",
    c."nom_département_structure",
    c."région_structure",
    c.nom_org_prescripteur,
    count(c."état") as nombre_de_candidatures
from
    {{ source('emplois', 'candidatures') }} as c
where
    c.type_structure in (
        'AI', 'ACI', 'EI', 'EITI', 'ETTI'
    )
group by
    c."état",
    c.date_candidature,
    c.id_structure,
    c.type_structure,
    c.nom_structure,
    c.injection_ai,
    c.origine,
    c."origine_détaillée",
    c."département_structure",
    c."nom_département_structure",
    c."région_structure",
    c.nom_org_prescripteur
