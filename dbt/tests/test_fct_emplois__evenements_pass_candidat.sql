with evenements as (
    select
        'prolongation'        as type_evenement,
        id_prolongation::text as id_evenement,
        id_pass,
        id_candidat
    from {{ ref('fct_emplois__prolongations') }}

    union all

    select
        'demande_prolongation'        as type_evenement,
        id_demande_prolongation::text as id_evenement,
        id_pass,
        id_candidat
    from {{ ref('fct_emplois__demandes_prolongation') }}

    union all

    select
        'suspension'        as type_evenement,
        id_suspension::text as id_evenement,
        id_pass,
        id_candidat
    from {{ ref('fct_emplois__suspensions') }}
)

select
    evenement.type_evenement,
    evenement.id_evenement,
    evenement.id_pass,
    evenement.id_candidat,
    pass.id_candidat as id_candidat_attendu
from evenements as evenement
left join {{ ref('fct_emplois__pass') }} as pass
    on evenement.id_pass = pass.id_pass
where evenement.id_candidat is distinct from pass.id_candidat
