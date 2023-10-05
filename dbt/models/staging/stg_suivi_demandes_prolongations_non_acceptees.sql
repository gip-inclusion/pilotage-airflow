select
    demandes_prolong."id_pass_agrément",
    demandes_prolong."date_début",
    demandes_prolong.date_fin,
    demandes_prolong.motif,
    demandes_prolong."état",
    demandes_prolong.date_de_demande,
    demandes_prolong.date_traitement,
    demandes_prolong.date_envoi_rappel,
    demandes_prolong.motif_de_refus,
    o.nom                                                                 as nom_prescripteur,
    o.type_complet                                                        as type_prescripteur,
    o."nom_département"                                                   as "département_prescripteur",
    o."région"                                                            as "région_prescripteur",
    s.nom                                                                 as nom_structure,
    s.nom_complet                                                         as nom_complet_structure,
    s.type                                                                as type_structure,
    s."nom_département"                                                   as "département_structure",
    s."région"                                                            as "région_structure",
    to_date('2999-08-06', 'YYYY-MM-DD')                                   as "date_de_création",
    case
        when pass.injection_ai = 0 then 'Non'
        else 'Oui'
    end                                                                   as reprise_de_stock_ai,
    /* delai_traitement is in days*/
    (demandes_prolong.date_traitement - demandes_prolong.date_de_demande) as delai_traitement,
    (current_date - demandes_prolong.date_de_demande)                     as duree_depuis_demande
from {{ source('emplois', 'demandes_de_prolongation') }} as demandes_prolong
left join {{ ref('stg_organisations') }} as o
    on demandes_prolong.id_organisation_prescripteur = o.id
left join {{ source('emplois', 'structures') }} as s
    on demandes_prolong."id_structure_déclarante" = s.id
left join {{ source('emplois', 'pass_agréments') }} as pass
    on demandes_prolong."id_pass_agrément" = pass.id
/* Sometimes you find duplicates, in the pass_agréments table, either on the id
or hash_pass_iae. The "bad" duplicate always never has an id_candidat.
Therefore we can remove these duplicate by filtering them */
where pass.id_candidat is not null and demandes_prolong."état" != 'Acceptée'
