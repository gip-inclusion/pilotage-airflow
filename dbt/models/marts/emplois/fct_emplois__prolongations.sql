with prolongations as (
    select *
    from {{ ref('stg_emplois__prolongations') }}
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
        prolongation.id_prolongation,
        prolongation.id_pass,
        pass.id_candidat,
        {{ date_id_from_date("prolongation.date_debut::date") }}    as date_debut_id,
        {{ date_id_from_date("prolongation.date_fin::date") }}      as date_fin_id,
        prolongation.motif,
        prolongation.id_utilisateur_declarant,
        structure_declarante.id                                     as id_structure_declarante,
        organisation_prescripteur.id                                as id_organisation_prescripteur,
        prolongation.id_utilisateur_prescripteur,
        {{ date_id_from_date("prolongation.date_creation::date") }} as date_creation_id,
        prolongation.id_demande_de_prolongation,
        prolongation.date_mise_a_jour_metabase
    from prolongations as prolongation
    left join pass
        on prolongation.id_pass = pass.id_pass
    left join etablissements as structure_declarante
        on prolongation.id_structure_declarante = structure_declarante.id_structure
    left join etablissements as organisation_prescripteur
        on prolongation.id_organisation_prescripteur = organisation_prescripteur.id_organisation
)

select *
from final
