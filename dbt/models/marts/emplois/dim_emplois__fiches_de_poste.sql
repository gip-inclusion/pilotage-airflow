with fiches_de_poste as (
    select *
    from {{ ref('stg_emplois__fiches_de_poste') }}
),

etablissements as (
    select *
    from {{ ref('dim_emplois__etablissements') }}
),

final as (
    select -- noqa: ST06
        fiche.id                                                     as id_fiche_de_poste,
        fiche.code_rome,
        fiche.nom_rome,
        fiche.type_contrat,
        fiche.recrutement_ouvert,
        etablissement.id                                             as id_employeur,
        fiche.date_creation,
        fiche.date_derniere_modification,
        fiche.date_mise_a_jour_metabase,
        coalesce(fiche.code_insee_poste, fiche.code_insee_employeur) as code_commune_poste,
        etablissement.is_siae                                        as employeur_is_siae
    from fiches_de_poste as fiche
    left join etablissements as etablissement
        on fiche.id_employeur = etablissement.id_structure
)

select *
from final
