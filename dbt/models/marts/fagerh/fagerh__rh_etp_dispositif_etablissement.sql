with etablissements as (

    select *
    from {{ ref('int_fagerh__etablissements') }}

),

final as (

    select
        uuid,
        finess,
        establishment_name,
        departement,

        esrp_etp,
        espo_etp,
        ueros_etp,
        deac_etp

    from etablissements

)

select *
from final
