/*

L'objectif est de créer une table agrégée sur les candidats et leur candidatures qui ne contient que les préscripteurs comme auteurs de diagnostics.
Nous récupérons aussi différentes informations sur les structures à partir de la table organisation afin de mettre en place des filtres + précis

*/
with candidats_p as (
    /* Ici on sélectionne les colonnes pertinentes à partir de la table candidats en ne prenant que les auteurs = Prescripteur */
    select distinct
        cdd.id                                as id_candidat,
        /* TODO dejafait drop as soon as analistos have migrated to the new deanonymized column */
        cdd."id_anonymisé"                    as id_candidat_anonymise,
        cdd.actif,
        cdd.age,
        cdd.date_diagnostic,
        cdd."département"                     as departement_candidat,
        cdd."nom_département"                 as nom_departement_candidat,
        cdd."région"                          as region_candidat,
        cdd.type_auteur_diagnostic,
        cdd.sous_type_auteur_diagnostic,
        cdd.nom_auteur_diagnostic,
        cdd.id_auteur_diagnostic_prescripteur as id_org_prescripteur,
        cdd.total_candidatures,
        cdd.total_diagnostics,
        cdd.total_embauches,
        cdd.type_inscription,
        cdd.injection_ai,
        cdd.pe_inscrit,
        case
            /* On soustrait 6 mois à la date de diagnostic pour déterminer s'il est toujours en cours ou pas */
            when date_diagnostic >= date_trunc('month', current_date) - interval '5 months' then 'Oui'
            else 'non'
        end                                   as diagnostic_valide
    from
        {{ source('emplois', 'candidats') }} as cdd /* cdd pour CanDiDats */
    where
        type_auteur_diagnostic = ('Prescripteur')
)

select
    /* On selectionne les colonnes finales qui nous intéressent */
    id_candidat,
    /* TODO dejafait drop as soon as analistos have migrated to the new deanonymized column */
    id_candidat_anonymise           as id_candidat_anonymise,
    actif,
    age,
    date_diagnostic,
    diagnostic_valide,
    departement_candidat,
    nom_departement_candidat,
    region_candidat,
    type_auteur_diagnostic,
    nom_auteur_diagnostic,
    id_org_prescripteur,
    candidats_p.total_candidatures,
    candidats_p.total_diagnostics,
    candidats_p.total_embauches,
    type_inscription,
    pe_inscrit,
    injection_ai,
    organisations_libelles.libelle  as type_auteur_diagnostic_detaille,
    prescripteurs.type_prescripteur as type_prescripteur,
    prescripteurs."nom_département" as "nom_département_prescripteur",
    prescripteurs."région"          as "nom_région_prescripteur",
    case
        /* ajout d'une colonne permettant de calculer le taux de candidats acceptées tout en faisant une jointure avec la table candidatures */
        when candidats_p.total_embauches > 0 then concat(cast(id_candidat as varchar), '_accepté')
    end                             as "candidature_acceptée"
from
    candidats_p
left join {{ ref('stg_organisations') }} as prescripteurs
    on
        prescripteurs.id = candidats_p.id_org_prescripteur
left join {{ ref('organisations_libelles') }} as organisations_libelles
    on candidats_p.sous_type_auteur_diagnostic = concat('Prescripteur ', organisations_libelles.type)
