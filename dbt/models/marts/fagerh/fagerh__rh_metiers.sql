with metiers as (

    select *
    from {{ ref('int_fagerh__metiers') }}

),

etablissements as (

    select *
    from {{ ref('int_fagerh__etablissements') }}

),

final as (

    select
        metiers.uuid,

        etablissements.finess,
        etablissements.establishment_name,
        etablissements.departement,

        metiers.metier,
        metiers.metier_family,
        metiers.metier_family_label,
        metiers.employment_mode,

        metiers.etp_cdi,
        metiers.etp_cdd,
        metiers.etp_total

    from metiers

    left join etablissements
        on metiers.uuid = etablissements.uuid

    where metiers.metier is not null

)

select *
from final
