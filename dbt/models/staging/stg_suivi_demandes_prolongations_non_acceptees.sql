select
    demandes_prolong."id_pass_agrément",
    pass."date_début",
    pass.date_fin,
    demandes_prolong.motif,
    demandes_prolong."état",
    demandes_prolong.date_de_demande,
    demandes_prolong.date_traitement,
    demandes_prolong.date_envoi_rappel,
    o.nom                                                                                   as nom_prescripteur,
    o.type_complet                                                                          as type_prescripteur,
    o."nom_département"                                                                     as "département_prescripteur",
    o."région"                                                                              as "région_prescripteur",
    s.nom                                                                                   as nom_structure,
    s.nom_complet                                                                           as nom_complet_structure,
    s.type                                                                                  as type_structure,
    s."nom_département_c1"                                                                  as "département_structure",
    s."région_c1"                                                                           as "région_structure",
    s."nom_département_c1"                                                                  as "département_structure_filtre",
    s."région_c1"                                                                           as "région_structure_filtre",
    demandes_prolong.date_de_demande                                                        as "date_de_création",
    case
        when demandes_prolong.motif_de_refus = 'IAE'
            then 'L’IAE ne correspond plus aux besoins / à la situation de la personne'
        when demandes_prolong.motif_de_refus = 'SIAE'
            then 'La typologie de SIAE ne correspond plus aux besoins / à la situation de la personne'
        when demandes_prolong.motif_de_refus = 'DURATION'
            then 'La durée de prolongation demandée n’est pas adaptée à la situation du candidat'
        when demandes_prolong.motif_de_refus = 'REASON'
            then 'Le motif de prolongation demandé n’est pas adapté à la situation du candidat.'
        else 'Pas de donnée disponible'
    end                                                                                     as motif_de_refus,
    case
        when pass.injection_ai = 0 then 'Non'
        else 'Oui'
    end                                                                                     as reprise_de_stock_ai,
    /* delai_traitement is in days*/
    extract(day from (demandes_prolong.date_traitement - demandes_prolong.date_de_demande)) as delai_traitement,
    /* we use the udpate day as reference in order to have an unity
    across all plots in our dashboards. Using the current day would lead to differences
    between some plots*/
    (demandes_prolong."date_mise_à_jour_metabase" - demandes_prolong.date_de_demande)       as duree_depuis_demande
from {{ source('emplois', 'demandes_de_prolongation') }} as demandes_prolong
left join {{ ref('stg_organisations') }} as o
    on demandes_prolong.id_organisation_prescripteur = o.id
left join {{ ref('structures') }} as s
    on demandes_prolong."id_structure_déclarante" = s.id
left join {{ source('emplois', 'pass_agréments') }} as pass
    on demandes_prolong."id_pass_agrément" = pass.id
/* Sometimes you find duplicates, in the pass_agréments table, either on the id
or hash_pass_iae. The "bad" duplicate always never has an id_candidat.
Therefore we can remove these duplicate by filtering them */
where pass.id_candidat is not null and demandes_prolong."état" != 'Acceptée'
