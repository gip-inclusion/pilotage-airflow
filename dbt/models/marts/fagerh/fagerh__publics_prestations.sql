with publics as (

    select *
    from {{ ref('int_fagerh__prestations_publics') }}

),

etablissements as (

    select *
    from {{ ref('int_fagerh__etablissements') }}

),

final as (

    select
        publics.uuid,

        etablissements.finess,
        etablissements.establishment_name,
        etablissements.departement,

        publics.prestation_key_base,
        publics.prestation_group,
        publics.prestation_label,
        publics.orp_status,
        publics.is_reliable_prestation_mapping,
        publics.is_unmapped_prestation,

        publics.public_dimension,
        publics.public_subdimension,
        publics.public_category_code,
        publics.public_category_label,

        publics.public_count

    from publics

    left join etablissements
        on publics.uuid = etablissements.uuid

    where publics.prestation_done is true

)

select *
from final
