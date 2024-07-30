/*

L'objectif est de créer une table agrégée sur les candidats
    et leur candidatures qui ne contient que les préscripteurs comme auteurs de diagnostics.
Nous récupérons aussi différentes informations sur les structures
    à partir de la table organisation afin de mettre en place des filtres + précis

*/
with candidats_p as (
    /* Ici on sélectionne les colonnes pertinentes à partir
        de la table candidats en ne prenant que les auteurs = Prescripteur */
    select distinct
        cdd.id                                as id_candidat,
        cdd.actif,
        cdd.age,
        cdd.tranche_age,
        cdd.sexe_selon_nir,
        cdd.date_diagnostic,
        cdd."département"                     as departement_candidat,
        cdd."nom_département"                 as nom_departement_candidat,
        cdd."région"                          as region_candidat,
        cdd."type_structure_dernière_embauche",
        cdd.type_org,
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
            when cdd.date_diagnostic >= date_trunc('month', current_date) - interval '5 months' then 'Oui'
            else 'non'
        end                                   as diagnostic_valide
    from
        {{ ref('candidats') }} as cdd /* cdd pour CanDiDats */
    where
        cdd.type_auteur_diagnostic = ('Prescripteur')
)

select
    /* On selectionne les colonnes finales qui nous intéressent */
    c.*,
    organisations_libelles.label as type_auteur_diagnostic_detaille,
    prescripteurs.type_prescripteur,
    prescripteurs.zone_emploi    as bassin_emploi_prescripteur,
    prescripteurs.epci           as epci_prescripteur,
    prescripteurs."dept_org"     as "nom_département_prescripteur",
    prescripteurs."région_org"   as "nom_région_prescripteur",
    case
        /* ajout d'une colonne permettant de calculer le taux de candidats acceptées
            tout en faisant une jointure avec la table candidatures */
        when c.total_embauches > 0 then concat(cast(c.id_candidat as varchar), '_accepté')
    end                          as "candidature_acceptée"
from
    candidats_p as c
left join {{ ref('stg_organisations') }} as prescripteurs
    on
        c.id_org_prescripteur = prescripteurs.id
left join {{ source('emplois','c1_ref_type_prescripteur') }} as organisations_libelles
    on c.type_org = organisations_libelles.code
