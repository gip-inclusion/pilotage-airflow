with demandes_prolongation as (
    select *
    from {{ ref('stg_emplois__demandes_prolongation') }}
),

pass as (
    select *
    from {{ ref('fct_emplois__pass') }}
),

etablissements as (
    select *
    from {{ ref('dim_emplois__etablissements') }}
),

final as (
    select
        demande.id_demande_prolongation,
        demande.id_pass,
        pass.id_candidat,
        demande.date_debut,
        demande.date_fin,
        demande.motif,
        demande.id_utilisateur_declarant,
        structure_declarante.id      as id_structure_declarante,
        organisation_prescripteur.id as id_organisation_prescripteur,
        demande.id_utilisateur_prescripteur,
        demande.id_prolongation,
        demande.etat,
        demande.motif_de_refus,
        demande.date_de_demande,
        demande.date_traitement,
        demande.id_utilisateur_traitant,
        demande.date_envoi_rappel,
        demande.date_mise_a_jour_metabase
    from demandes_prolongation as demande
    left join pass
        on demande.id_pass = pass.id_pass
    left join etablissements as structure_declarante
        on demande.id_structure_declarante = structure_declarante.id_structure
    left join etablissements as organisation_prescripteur
        on demande.id_organisation_prescripteur = organisation_prescripteur.id_organisation
)

select *
from final
