/* L 'objectif est de créer une table agrégée avec le nombre de candidatures et le taux de candidature
selon l'état de la candidature, la structure, le type de structure, l'orgine de la candidature et le prescripteur */

select
    c."état",
    c.date_candidature,
    c.nombre_de_candidatures, -- nombre de candidatures pour cette structure, état, date, origine et prescripteur donné
    prop_cddr.total_candidatures, -- nombre total de candidatures pour cette structure et cet état
    ttes_ccdr.somme_candidatures, -- nombre total de candidatures pour cette structure
    ttes_ccdr_ph.somme_candidatures_ph, -- nombre de candidatures pour cette structure et ce prescripteur
    c.nombre_autoprescription, -- nombre de candidatures en autoprescription pour cet état et cette structure
    c.nom_structure,
    c.type_structure,
    c.origine,
    c."origine_détaillée",
    c."département_structure",
    c."nom_département_structure",
    c."région_structure",
    c.nom_org_prescripteur,
    c.id_structure,
    c.injection_ai,
    s.ville,
    s.siret,
    s.nom_epci_structure,
    /* calcul de la proportion de candidatures en % */
    c.nombre_de_candidatures / prop_cddr.total_candidatures as taux_de_candidatures -- taux de candidatures à l'état pour la structure
from
    {{ ref('eph_nbr_candidatures') }} as c
left join {{ ref('eph_prop_cddr') }} as prop_cddr
    on
        c."état" = prop_cddr."état"
        and c.id_structure = prop_cddr.id_structure
left join {{ ref('eph_nbr_cddr_structures') }} as ttes_ccdr
    on
        c.id_structure = ttes_ccdr.id_structure
left join {{ ref('eph_nbr_cddr_prescripteurs') }} as ttes_ccdr_ph
    on
        c.id_structure = ttes_ccdr_ph.id_structure
        and c.nom_org_prescripteur = ttes_ccdr_ph.nom_org_prescripteur
left join {{ ref('structures') }} as s
    on
        c.id_structure = s.id
