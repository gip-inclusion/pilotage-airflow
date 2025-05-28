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
    count(c."état")                                                                   as nombre_de_candidatures,
    count(case when type_de_candidature = 'Autoprescription' then id_candidature end) as nombre_autoprescription
from
    {{ ref('stg_candidatures_autoprescription') }} as c
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
