with suspensions as (
    select *
    from {{ ref('stg_emplois__suspensions') }}
),

pass_iae as (
    select *
    from {{ ref('fct_emplois__pass') }}
),

final as (
    select
        suspension.id_suspension,
        suspension.id_pass,
        pass.id_candidat,
        suspension.date_debut,
        suspension.date_fin,
        suspension.motif,
        suspension.en_cours,
        suspension.date_creation,
        suspension.date_mise_a_jour_metabase
    from suspensions as suspension
    left join pass_iae as pass
        on suspension.id_pass = pass.id_pass
)

select *
from final
